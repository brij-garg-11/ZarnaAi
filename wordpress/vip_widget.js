/*
 * Zarna VIP bracelet widget — front-end wiring (international / WhatsApp ready).
 *
 * Injected as a <script> at the end of the .zg-vip block on the VIP page
 * (WPBakery Raw HTML). It self-bootstraps the intl-tel-input library from a CDN
 * (searchable country-flag picker), then drives the "Text Alerts" step:
 *
 *   1. Fan picks their country + types their number  → we build clean E.164.
 *   2. Tap "Sign up"  → opens a PRE-FILLED deep link to 855-608-1717:
 *        • US / Canada → SMS  (native Messages, prefilled keyword)
 *        • everywhere else → WhatsApp (wa.me, prefilled keyword)
 *      US toll-free numbers are unreliable for international SMS, so non-US
 *      fans are routed to WhatsApp, which works worldwide to the same number.
 *   3. Fan taps Send in their messaging app → that inbound text enrolls them.
 *   4. Back on the page, we VERIFY via the same-origin proxy
 *      (/wp-json/zarna/v1/verify → Railway /verify/signup). The step only turns
 *      green once the bot has actually recorded the signup for the live show.
 *
 * Steps 1 (Newsletter) and 3 (YouTube) are intentionally left inert until their
 * integrations are built; the claim block stays gated on all three.
 */
(function () {
  var CFG = {
    proxy:      '/wp-json/zarna/v1/verify',
    smsNumber:  '18556081717',        // E.164 digits, no '+', no punctuation
    smsDisplay: '(855) 608-1717',
    // TODO(brij): pick the real enrollment keyword — MUST match whatever the bot
    // treats as the live-show signup trigger. Change this one value when decided.
    keyword:    'JOIN',
    smsCountries: ['us', 'ca'],       // route these to native SMS; all else WhatsApp
    iti: {
      css: 'https://cdn.jsdelivr.net/npm/intl-tel-input@25.14.1/build/css/intlTelInput.css',
      js:  'https://cdn.jsdelivr.net/npm/intl-tel-input@25.14.1/build/js/intlTelInput.min.js',
      utils: 'https://cdn.jsdelivr.net/npm/intl-tel-input@25.14.1/build/js/utils.js'
    },
    store: 'zgVip.v1'
  };

  var byId = function (id) { return document.getElementById(id); };
  var STEPS = {
    news: { step: 'zg-step-news', pip: 'zg-pip-0' },
    sms:  { step: 'zg-step-sms',  pip: 'zg-pip-1' },
    yt:   { step: 'zg-step-yt',   pip: 'zg-pip-2' }
  };

  function load()  { try { return JSON.parse(localStorage.getItem(CFG.store)) || {}; } catch (e) { return {}; } }
  function save(s) { try { localStorage.setItem(CFG.store, JSON.stringify(s)); } catch (e) {} }
  var state = load();

  function setStatus(el, msg, cls) {
    if (!el) return;
    el.textContent = msg || '';
    el.className = 'zg-status' + (cls ? ' ' + cls : '');
  }

  function markDone(key) {
    state[key] = true; save(state);
    var s = STEPS[key];
    var stepEl = byId(s.step), pipEl = byId(s.pip);
    if (stepEl) stepEl.classList.add('zg-done');
    if (pipEl)  pipEl.classList.add('zg-done');
    if (state.news && state.sms && state.yt) {
      var claim = byId('zg-claim');
      if (claim) claim.classList.add('zg-show');
    }
  }

  // Restore progress from a previous visit (same device/browser).
  Object.keys(STEPS).forEach(function (k) { if (state[k]) markDone(k); });

  // --- load the intl-tel-input assets, then wire the SMS step ------------- //
  function loadCss(href) {
    if (document.querySelector('link[href="' + href + '"]')) return;
    var l = document.createElement('link');
    l.rel = 'stylesheet'; l.href = href;
    document.head.appendChild(l);
  }
  function loadScript(src, cb) {
    if (window.intlTelInput) { cb(); return; }
    var existing = document.querySelector('script[data-iti="1"]');
    if (existing) { existing.addEventListener('load', cb); return; }
    var s = document.createElement('script');
    s.src = src; s.async = true; s.setAttribute('data-iti', '1');
    s.onload = cb;
    s.onerror = function () { wireSms(null); };  // degrade gracefully to manual entry
    document.head.appendChild(s);
  }
  // Make the picker span the field width (iti wraps the input in .iti).
  function injectStyle() {
    if (byId('zg-iti-style')) return;
    var st = document.createElement('style');
    st.id = 'zg-iti-style';
    st.textContent = '.zg-vip .iti{width:100%}';
    document.head.appendChild(st);
  }

  var phoneInput = byId('zg-phone');
  if (phoneInput) {
    injectStyle();
    loadCss(CFG.iti.css);
    loadScript(CFG.iti.js, function () {
      var iti = null;
      try {
        iti = window.intlTelInput(phoneInput, {
          initialCountry: 'us',
          countrySearch: true,                 // searchable dropdown (v25 default)
          loadUtils: function () { return import(CFG.iti.utils); }
        });
      } catch (e) { iti = null; }
      wireSms(iti);
    });
  }

  // --- SMS step state machine --------------------------------------------- //
  // idle  → tap "Sign up"        : open prefilled deep link, go to 'sent'
  // sent  → tap "I sent it"      : verify against the live show; green on success
  function wireSms(iti) {
    var btn = byId('zg-phone-btn');
    var st  = byId('zg-phone-status');
    if (!btn) return;

    if (state.sms) return;  // already verified this device — nothing to wire

    var LABEL_IDLE = btn.getAttribute('data-idle-label') || btn.textContent || 'Sign up';
    var LABEL_SENT = 'I sent it — check me';

    function e164() {
      // Prefer the library's canonical E.164 (needs utils, loaded post-init).
      if (iti && typeof iti.getNumber === 'function') {
        var n = (iti.getNumber() || '').replace(/\s+/g, '');
        if (/^\+\d{8,15}$/.test(n)) return n;
      }
      // Fallback: selected dial code + typed national digits (US default if none).
      var cc = (iti && iti.getSelectedCountryData && (iti.getSelectedCountryData().dialCode || '')) || '';
      var typed = (phoneInput.value || '').trim();
      var digits = typed.replace(/\D/g, '');
      if (typed.charAt(0) === '+') return /^\+\d{8,15}$/.test('+' + digits) ? '+' + digits : null;
      if (cc) {
        if (digits.indexOf(cc) === 0 && digits.length > cc.length) digits = digits.slice(cc.length);
        var full = '+' + cc + digits;
        return /^\+\d{8,15}$/.test(full) ? full : null;
      }
      if (digits.length === 10) return '+1' + digits;
      if (digits.length === 11 && digits.charAt(0) === '1') return '+' + digits;
      return null;
    }

    function isUsOrCa() {
      var iso = (iti && iti.getSelectedCountryData && (iti.getSelectedCountryData().iso2 || '')) || '';
      if (iso) return CFG.smsCountries.indexOf(iso.toLowerCase()) !== -1;
      // No picker → infer from the number: +1 = NANP.
      var n = e164() || '';
      return n.indexOf('+1') === 0;
    }

    function openJoin(phone) {
      var msg = encodeURIComponent(CFG.keyword);
      var isApple = /iPhone|iPad|iPod|Macintosh/i.test(navigator.userAgent);
      var link, newTab = false;
      if (isUsOrCa()) {
        // iOS wants "&body=" after the address; Android/others want "?body=".
        link = 'sms:+' + CFG.smsNumber + (isApple ? '&' : '?') + 'body=' + msg;
      } else {
        link = 'https://wa.me/' + CFG.smsNumber + '?text=' + msg;
        newTab = true;                          // keep the VIP page open behind WhatsApp
      }
      var a = document.createElement('a');
      a.href = link;
      if (newTab) { a.target = '_blank'; a.rel = 'noopener'; }
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    }

    function toSent() {
      btn.textContent = LABEL_SENT;
      var channel = isUsOrCa() ? 'Messages' : 'WhatsApp';
      setStatus(st, 'We opened ' + channel + ' with your join text ready — tap Send there, ' +
                    'then come back and tap "' + LABEL_SENT + '".', 'zg-load');
    }

    function verify() {
      var phone = state.smsE164 || e164();
      if (!phone) { setStatus(st, 'Enter a valid phone number.', 'zg-err'); return; }
      btn.disabled = true;
      setStatus(st, 'Checking\u2026', 'zg-load');
      fetch(CFG.proxy, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ phone: phone })
      })
        .then(function (r) { return r.json().catch(function () { return {}; }); })
        .then(function (d) {
          btn.disabled = false;
          if (d && d.subscribed) {
            setStatus(st, 'You\u2019re in \u2713', 'zg-ok');
            state.smsPending = false; save(state);
            markDone('sms');
          } else {
            setStatus(st, 'We don\u2019t see you yet \u2014 tap Send in your messaging app, ' +
                          'give it a few seconds, then tap "' + LABEL_SENT + '" again.', 'zg-err');
          }
        })
        .catch(function () {
          btn.disabled = false;
          setStatus(st, 'Couldn\u2019t check right now \u2014 please try again.', 'zg-err');
        });
    }

    // If they left mid-flow last time, resume in the "sent" state so they can verify.
    if (state.smsPending) toSent();

    btn.addEventListener('click', function () {
      if (state.smsPending) { verify(); return; }
      var phone = e164();
      if (!phone) { setStatus(st, 'Enter a valid phone number.', 'zg-err'); return; }
      if (iti && typeof iti.isValidNumber === 'function' && iti.isValidNumber() === false) {
        setStatus(st, 'That number doesn\u2019t look right \u2014 double-check it.', 'zg-err');
        return;
      }
      state.smsE164 = phone; state.smsPending = true; save(state);
      openJoin(phone);
      toSent();
    });

    // Auto-verify when they return to the tab after sending (nice-to-have).
    document.addEventListener('visibilitychange', function () {
      if (document.visibilityState === 'visible' && state.smsPending && !state.sms) {
        verify();
      }
    });
  }
})();
