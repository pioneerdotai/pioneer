(() => {
  // Theme colors only: no native IPC, grant, or other capability is exposed.
  // Installed Apps run in an opaque sandbox, but location.origin still names
  // the loaded URL. Do not apply the App surface to unrelated subframes.
  const origin = __PIONEER_ORIGIN__;
  if (location.origin !== origin) return;
  const theme = __PIONEER_THEME__;
  const key = Symbol.for('pioneer.theme.surface');
  if (!document[key]) {
    // CSSOM leaves React's HTML/head/body markup untouched during hydration.
    const sheet = new CSSStyleSheet();
    sheet.replaceSync(':root {}');
    document.adoptedStyleSheets = [...document.adoptedStyleSheets, sheet];
    const style = sheet.cssRules[0].style;
    const apply = value => {
      const colors = value?.colors;
      if (!['light', 'dark'].includes(value?.mode) ||
          typeof colors?.background !== 'string' ||
          typeof colors?.foreground !== 'string' ||
          !CSS.supports('color', colors.background) ||
          !CSS.supports('color', colors.foreground)) return;
      style.setProperty('--pioneer-background', colors.background);
      style.setProperty('--pioneer-foreground', colors.foreground);
      style.setProperty('--pioneer-color-scheme', value.mode);
      style.setProperty('--pioneer-host-theme', '1');
    };
    document[key] = apply;
    window.addEventListener('message', event => {
      if (window.parent === window) {
        if (event.source !== window || event.origin !== origin) return;
      } else if (event.source !== window.parent) return;
      if (event.data?.type === 'pioneer.theme') apply(event.data.theme);
    });
  }
  document[key](theme);
})();
