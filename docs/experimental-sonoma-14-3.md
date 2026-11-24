# Experimental macOS 14.3 build

The `experimental-sonoma-14-3` desktop feature admits exactly macOS 14.3.0
with Safari 17.3. It exists for a user's isolated compatibility experiment.
Default builds retain the upstream admission policy. The experimental build
continues to recommend a system update and does not repair old WebKit security
vulnerabilities. It must not be treated as a security-reviewed browser.

Build the app bundle with `pnpm -C desktop exec tauri build --bundles app
--features experimental-sonoma-14-3`. It is locally signed rather than
notarized. Keep experiments in a separate profile. Update Sonoma and Safari
and return to a standard build for ordinary browsing.
