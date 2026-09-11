/**
 * The Worker's bindings and secrets.
 *
 * Declared by hand rather than committing `wrangler types` output: that file is hundreds of
 * kilobytes of runtime type definitions regenerated on every `wrangler.jsonc` edit, and a template
 * a tournament director clones should not carry it. The bindings a deployment actually has are
 * below, and CI checks them by building the Worker.
 */

declare namespace Cloudflare {
  interface Env {
    QBTCP_RELAY: DurableObjectNamespace<import('./relay').QbtcpRelay>;
    /**
     * The one-time setup token this deployment will accept in exchange for a management
     * credential.
     *
     * A Worker secret, set by the deploying tournament operator. Absent means the backend refuses
     * to be claimed at all, which is the right posture for a deployment that was never finished.
     */
    RELAY_SETUP_TOKEN?: string;
    /**
     * Comma-separated browser origins allowed to call authenticated endpoints and to open the
     * stream, e.g. `https://scorer.example,https://director.example`.
     *
     * Requests without an `Origin` (native apps, curl, Director sync jobs) are unaffected.
     * The single value `*` allows any origin explicitly; there is no implicit wildcard.
     */
    RELAY_ALLOWED_ORIGINS?: string;
  }
}

interface Env extends Cloudflare.Env {}
