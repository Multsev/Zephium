# Yandex Browser bookmarks

Zephium detects Yandex Browser profiles that contain a Chromium-format
`Bookmarks` or `AccountBookmarks` file in the standard macOS or Windows user
data directory. Select Яндекс Браузер and a profile in browser-data import;
only bookmarks are offered. History is not qualified or imported.

Bookmarks from the bar are imported into the source folder; nonempty Other,
Mobile and Yandex's Collections roots retain their own folders. Unsupported
URLs and empty folders follow Zephium's existing import rules. Repeating an
import merges matching folders and skips an address already in that folder.
If a source file cannot be read or parsed the import reports an error rather
than replacing the destination. The source files are read only.
