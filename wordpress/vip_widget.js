/*
 * Zarna VIP bracelet widget — front-end wiring.
 *
 * This is injected as a <script> at the end of the .zg-vip block on the VIP page
 * (WPBakery Raw HTML). It toggles the class vocabulary the widget CSS already
 * defines:  zg-done (step + pip), zg-show (claim), zg-ok/zg-err/zg-load (status).
 *
 * Scope: SMS-only. Step 2 (SMS) POSTs the phone to the same-origin WordPress
 * proxy (/wp-json/zarna/v1/verify), which forwards to the Railway /verify/signup
 * endpoint. Steps 1 (Newsletter) and 3 (YouTube) are left inert until their
 * integrations are built; the claim block stays gated on all three.
 */
(function () {
  var CFG = {
    proxy:      '/wp-json/zarna/v1/verify',
    smsDisplay: '(855) 608-1717',
    store:      'zgVip.v1'
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

  // NOTE: Scope is SMS-only for now. Steps 1 (Newsletter) and 3 (YouTube) are
  // intentionally left inert until their integrations are built. The claim block
  // stays gated on all three, so it won't reveal until those are wired.

  // ---- Step 2: SMS (server-verified against the currently-live show) ----
  function normUS(raw) {
    var d = (raw || '').replace(/\D/g, '');
    if (d.length === 10) return '+1' + d;
    if (d.length === 11 && d.charAt(0) === '1') return '+' + d;
    return null;
  }

  var phoneBtn = byId('zg-phone-btn');
  if (phoneBtn) phoneBtn.addEventListener('click', function () {
    var st = byId('zg-phone-status');
    var phone = normUS(byId('zg-phone').value);
    if (!phone) { setStatus(st, 'Enter a valid US phone number.', 'zg-err'); return; }

    phoneBtn.disabled = true;
    setStatus(st, 'Checking\u2026', 'zg-load');

    fetch(CFG.proxy, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ phone: phone })
    })
      .then(function (r) { return r.json().catch(function () { return {}; }); })
      .then(function (d) {
        phoneBtn.disabled = false;
        if (d && d.subscribed) {
          setStatus(st, 'You\u2019re in \u2713', 'zg-ok');
          markDone('sms');
        } else {
          setStatus(st, 'We don\u2019t see your number yet \u2014 text ' + CFG.smsDisplay +
                        ' to join, then tap Sign up again.', 'zg-err');
        }
      })
      .catch(function () {
        phoneBtn.disabled = false;
        setStatus(st, 'Couldn\u2019t check right now \u2014 please try again.', 'zg-err');
      });
  });
})();
