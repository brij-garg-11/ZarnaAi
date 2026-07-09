<?php
/**
 * Zarna VIP bracelet — server-side proxy for SMS signup verification.
 *
 * WHAT IT DOES
 *   Exposes  POST /wp-json/zarna/v1/verify   with a JSON body {"phone":"..."}.
 *   The browser (VIP page) calls THIS same-origin endpoint. This code adds the
 *   secret server-side and forwards to the Railway endpoint, so VERIFY_SECRET_KEY
 *   never appears in the browser. Returns only {"subscribed": true|false}.
 *
 * HOW TO INSTALL (one-time)
 *   1. WP admin → Code Snippets (WPCode) → Add Snippet → Add Your Custom Code (New Snippet).
 *   2. Type: "PHP Snippet".  Paste everything in this file (you may omit the opening <?php).
 *   3. Set the secret below (see SECRET). 
 *   4. Insert Method: "Auto Insert" → Location: "Run Everywhere".  Save + Activate.
 *
 * SECRET (pick ONE):
 *   A) BEST — add to wp-config.php:   define('ZARNA_VERIFY_SECRET', 'the-key');
 *      then leave the placeholder below as-is; this file will read the constant.
 *   B) SIMPLE — paste the key straight into $FALLBACK_SECRET below (admin-only visible).
 *   The key value is the VERIFY_SECRET_KEY set on the Railway "web" service.
 */

add_action('rest_api_init', function () {
    register_rest_route('zarna/v1', '/verify', array(
        'methods'             => 'POST',
        'permission_callback' => '__return_true', // public route; the secret is added server-side
        'callback'            => 'zarna_verify_signup_proxy',
    ));
});

function zarna_verify_signup_proxy(WP_REST_Request $request) {
    // ---- config ----
    $ENDPOINT        = 'https://web-production-ec3da.up.railway.app/verify/signup';
    $FALLBACK_SECRET = 'PASTE_VERIFY_SECRET_KEY_HERE'; // used only if the constant isn't defined
    $RATE_MAX        = 30;   // max checks per IP per window
    $RATE_WINDOW     = 60;   // seconds

    $secret = defined('ZARNA_VERIFY_SECRET') ? ZARNA_VERIFY_SECRET : $FALLBACK_SECRET;
    if (!$secret || $secret === 'PASTE_VERIFY_SECRET_KEY_HERE') {
        return new WP_REST_Response(array('subscribed' => false, 'error' => 'not_configured'), 503);
    }

    // ---- light per-IP rate limit (protects against enumeration) ----
    $ip  = isset($_SERVER['HTTP_X_FORWARDED_FOR'])
        ? trim(explode(',', $_SERVER['HTTP_X_FORWARDED_FOR'])[0])
        : (isset($_SERVER['REMOTE_ADDR']) ? $_SERVER['REMOTE_ADDR'] : 'unknown');
    $key = 'zarna_verify_rl_' . md5($ip);
    $hits = (int) get_transient($key);
    if ($hits >= $RATE_MAX) {
        return new WP_REST_Response(array('subscribed' => false, 'error' => 'rate_limited'), 429);
    }
    set_transient($key, $hits + 1, $RATE_WINDOW);

    // ---- validate input ----
    $phone = trim((string) $request->get_param('phone'));
    if ($phone === '') {
        return new WP_REST_Response(array('subscribed' => false, 'error' => 'phone_required'), 400);
    }

    // ---- forward to Railway with the secret in the header ----
    $resp = wp_remote_post($ENDPOINT, array(
        'timeout' => 8,
        'headers' => array(
            'Content-Type' => 'application/json',
            'X-Api-Key'    => $secret,
        ),
        'body'    => wp_json_encode(array('phone' => $phone)),
    ));

    if (is_wp_error($resp)) {
        return new WP_REST_Response(array('subscribed' => false, 'error' => 'upstream_unreachable'), 502);
    }

    $code = wp_remote_retrieve_response_code($resp);
    $body = json_decode(wp_remote_retrieve_body($resp), true);

    // Only ever hand the browser the boolean. Never leak upstream error detail or the secret.
    if ($code === 200 && is_array($body) && array_key_exists('subscribed', $body)) {
        return new WP_REST_Response(array('subscribed' => (bool) $body['subscribed']), 200);
    }
    return new WP_REST_Response(array('subscribed' => false, 'error' => 'verify_failed'), 200);
}
