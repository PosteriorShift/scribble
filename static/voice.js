/* Real-time voice chat over WebRTC (mesh) using the game's WebSocket for signaling.
   Usage:
     const voice = new Voice({ ws, myId, send, onStatus, onActivity });
     voice.enable();   // request mic + start connecting to current peers
     voice.disable();  // stop mic + close all peers
     voice.setPeers([{id}, ...]);  // called whenever room_state arrives
     voice.handleSignal(from, signal); // call from ws message handler
     voice.toggleMute(); voice.isMuted;
*/
(function (global) {
  "use strict";

  const ICE_SERVERS = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" },
    { urls: "stun:global.stun.twilio.com:3478" },
    // If you self-host coturn, add:
    // { urls: "turn:your.turn.server:3478", username: "user", credential: "pass" },
  ];

  class Voice {
    constructor(opts) {
      this.ws = opts.ws;
      this.myId = opts.myId;
      this.send = opts.send;                 // (msg) => void
      this.onStatus = opts.onStatus || (() => {});
      this.onActivity = opts.onActivity || (() => {});
      this.localStream = null;
      this.peers = new Map();                // id -> { pc, audioEl, analyser, data, polite, makingOffer, ignoreOffer, isSettingRemote, iceQueue }
      this.enabled = false;
      this.isMuted = false;
      this.remotePeers = new Set();
      this._raf = null;
      this._audioCtx = null;
    }

    /* ---------- public ---------- */

    async enable() {
      if (this.enabled) return true;
      try {
        this.localStream = await navigator.mediaDevices.getUserMedia({
          audio: {
            echoCancellation: true,
            noiseSuppression: true,
            autoGainControl: true,
          },
          video: false,
        });
      } catch (e) {
        console.warn("[voice] getUserMedia failed:", e);
        this.onStatus("denied");
        return false;
      }
      this.enabled = true;
      this.onStatus("on");
      // connect to everyone we already know about
      for (const id of this.remotePeers) this._ensurePeer(id);
      this._startActivityLoop();
      return true;
    }

    disable() {
      this.enabled = false;
      if (this.localStream) {
        this.localStream.getTracks().forEach((t) => t.stop());
        this.localStream = null;
      }
      for (const [id, p] of this.peers) {
        try { p.pc.close(); } catch (e) {}
        if (p.audioEl) p.audioEl.remove();
        if (p.rafId) cancelAnimationFrame(p.rafId);
      }
      this.peers.clear();
      if (this._raf) cancelAnimationFrame(this._raf);
      this._raf = null;
      this.onStatus("off");
    }

    toggleMute() {
      this.isMuted = !this.isMuted;
      if (this.localStream) {
        this.localStream.getAudioTracks().forEach((t) => (t.enabled = !this.isMuted));
      }
      return this.isMuted;
    }

    setPeers(peerList) {
      const ids = new Set(peerList.map((p) => p.id).filter((id) => id !== this.myId));
      // remove peers that left
      for (const id of [...this.peers.keys()]) {
        if (!ids.has(id)) this._dropPeer(id);
      }
      this.remotePeers = ids;
      if (this.enabled) {
        for (const id of ids) this._ensurePeer(id);
      }
    }

    handleSignal(from, signal) {
      if (!this.enabled) return;          // ignore if mic is off
      this._ensurePeer(from);
      this._applySignal(from, signal).catch((e) => console.warn("[voice] signal err", e));
    }

    /* ---------- internals ---------- */

    _ensurePeer(id) {
      if (this.peers.has(id)) return this.peers.get(id);
      // deterministic polite/impolite based on id ordering
      const polite = String(this.myId) < String(id);

      const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

      const audioEl = document.createElement("audio");
      audioEl.autoplay = true;
      audioEl.playsInline = true;
      audioEl.setAttribute("playsinline", "");
      audioEl.dataset.peerId = id;
      audioEl.style.display = "none";
      document.body.appendChild(audioEl);

      const state = {
        pc,
        audioEl,
        polite,
        makingOffer: false,
        ignoreOffer: false,
        isSettingRemote: false,
        iceQueue: [],
        remoteDescSet: false,
        connected: false,
      };
      this.peers.set(id, state);

      // add local tracks
      if (this.localStream) {
        this.localStream.getTracks().forEach((t) => pc.addTrack(t, this.localStream));
      }

      // Perfect negotiation
      pc.onnegotiationneeded = async () => {
        try {
          state.makingOffer = true;
          await pc.setLocalDescription();
          this._safeSend({ type: "voice_signal", target: id, signal: { sdp: pc.localDescription } });
        } catch (e) {
          console.warn("[voice] negotiation err", e);
        } finally {
          state.makingOffer = false;
        }
      };

      pc.onicecandidate = ({ candidate }) => {
        if (candidate) {
          this._safeSend({ type: "voice_signal", target: id, signal: { ice: candidate } });
        }
      };

      pc.onconnectionstatechange = () => {
        state.connected = pc.connectionState === "connected";
        this._emitStatusSnapshot();
      };

      pc.ontrack = (ev) => {
        audioEl.srcObject = ev.streams[0];
        // If autoplay blocked, try again; mobile requires user gesture.
        const tryPlay = () => audioEl.play().catch(() => {
          // retry on next user interaction
          const once = () => { audioEl.play().catch(() => {}); window.removeEventListener("touchstart", once); window.removeEventListener("click", once); };
          window.addEventListener("touchstart", once, { once: true });
          window.addEventListener("click", once, { once: true });
        });
        tryPlay();
      };

      // If we're the impolite (initiator) side and mic is on, kick off
      if (this.localStream && !polite) {
        // onnegotiationneeded will fire after addTrack
      } else if (this.localStream && polite) {
        // polite waits for offer; but if none comes within 1.5s, send our own
        setTimeout(() => {
          if (!state.connected && pc.signalingState === "stable") {
            pc.onnegotiationneeded && pc.onnegotiationneeded();
          }
        }, 1500);
      }

      return state;
    }

    async _applySignal(from, signal) {
      const state = this._ensurePeer(from);
      const pc = state.pc;

      if (signal.sdp) {
        const offerCollision =
          signal.sdp.type === "offer" &&
          (state.makingOffer || pc.signalingState !== "stable");

        state.ignoreOffer = !state.polite && offerCollision;
        if (state.ignoreOffer) return;

        state.isSettingRemote = true;
        await pc.setRemoteDescription(signal.sdp);
        state.isSettingRemote = false;
        state.remoteDescSet = true;

        // flush queued ICE
        for (const c of state.iceQueue) {
          try { await pc.addIceCandidate(c); } catch (e) {}
        }
        state.iceQueue = [];

        if (signal.sdp.type === "offer") {
          await pc.setLocalDescription();
          this._safeSend({ type: "voice_signal", target: from, signal: { sdp: pc.localDescription } });
        }
      } else if (signal.ice) {
        if (!state.remoteDescSet) {
          state.iceQueue.push(signal.ice);
        } else {
          try { await pc.addIceCandidate(signal.ice); } catch (e) {}
        }
      }
    }

    _dropPeer(id) {
      const p = this.peers.get(id);
      if (!p) return;
      try { p.pc.close(); } catch (e) {}
      if (p.audioEl) p.audioEl.remove();
      this.peers.delete(id);
    }

    _safeSend(msg) {
      try { this.send(msg); } catch (e) {}
    }

    _emitStatusSnapshot() {
      const total = this.peers.size;
      let connected = 0;
      for (const p of this.peers.values()) if (p.connected) connected++;
      this.onStatus(this.enabled ? "on" : "off", { connected, total });
    }

    /* ---------- local speaking detection ---------- */
    _startActivityLoop() {
      if (!this.localStream) return;
      if (!this._audioCtx) {
        const AC = window.AudioContext || window.webkitAudioContext;
        if (!AC) return;
        this._audioCtx = new AC();
      }
      const ctx = this._audioCtx;
      const src = ctx.createMediaStreamSource(this.localStream);
      const analyser = ctx.createAnalyser();
      analyser.fftSize = 512;
      src.connect(analyser);
      const data = new Uint8Array(analyser.frequencyBinCount);
      let lastSpeaking = false;
      let lastSent = 0;

      const loop = () => {
        analyser.getByteFrequencyData(data);
        let sum = 0;
        for (let i = 0; i < data.length; i++) sum += data[i];
        const avg = sum / data.length;
        const speaking = avg > 18 && !this.isMuted;
        const now = performance.now();
        if (speaking !== lastSpeaking && now - lastSent > 250) {
          lastSpeaking = speaking;
          lastSent = now;
          this._safeSend({ type: "voice_activity", speaking });
        }
        this._raf = requestAnimationFrame(loop);
      };
      this._raf = requestAnimationFrame(loop);
    }
  }

  global.Voice = Voice;
})(window);
