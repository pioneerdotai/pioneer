(() => {
  // Windows may inject Wry scripts into subframes: only the host document owns IPC.
  if (window.top !== window) return;
  const origin = __PIONEER_ORIGIN__;
  if (origin !== null && location.origin !== origin) return;
  const token = __PIONEER_TOKEN__;
  window.addEventListener('message', event => {
    if (origin !== null) {
      if (event.source !== window || event.origin !== origin) return;
    } else {
      const frame = document.querySelector('iframe');
      if (!frame || event.source !== frame.contentWindow) return;
    }
    const data = event.data;
    if (!data || typeof data.id !== 'string' || !/^[a-zA-Z0-9-]{1,64}$/.test(data.id)) return;
    if (data.type === 'pioneer.view.setPlacement' &&
        ['fullscreen', 'bottom', 'right'].includes(data.placement)) {
      window.ipc.postMessage(JSON.stringify({
        type: data.type, id: data.id, placement: data.placement, token,
      }));
    } else if (data.type === 'pioneer.view.close') {
      window.ipc.postMessage(JSON.stringify({ type: data.type, id: data.id, token }));
    }
  });
})();
