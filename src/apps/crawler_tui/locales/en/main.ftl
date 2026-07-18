app-title = NoPubSearch — Crawler

tabs-dashboard = Dashboard
tabs-logs = Logs
tabs-database-console = Database Console
tabs-configs = Configs
tabs-secrets-configs = Secrets Configs

dashboard-domains-visited = Visited domains
dashboard-pages-crawled = Crawled pages
dashboard-pages-per-minute = Pages / min
dashboard-urls-waiting = Waiting URLs
dashboard-recent-errors = Recent errors
dashboard-bloom-filter = URLs in bloom filter
dashboard-uptime = Uptime
dashboard-all-threads = All threads
dashboard-pause = ⏸ Pause
dashboard-resume = ▶ Resume
dashboard-stop = ⏹ Stop
dashboard-threads-running = Running threads

thread-running = ● Running
thread-paused = ● Paused
thread-stopped = ● Stopped
thread-pages-count = { $count ->
    [0] No pages
    [one] { $count } page
   *[other] { $count } pages
}
thread-active-seconds-ago = active { $seconds }s ago
thread-active-minutes-ago = active { $minutes }min ago
thread-pause = Pause
thread-resume = Resume
thread-stop = Stop

logs-select-source = Select a log source
logs-status-bar = Source: { $crawler }  ·  Min level: { $level }
logs-back = Back
logs-level-up = Level up (+)
logs-level-down = Level down (-)
logs-all-crawlers = All crawlers

database-admin-credentials-title = 🔒 PostgreSQL Admin Credentials
database-admin-username-placeholder = Admin username (e.g. postgres)
database-password-placeholder = Password
database-save-and-access = Save and Access
database-testing-connection = Testing connection...
database-console-title = PostgreSQL Console (Restricted to database: { $db_name })
database-edit-credentials = Edit credentials
database-execute-f5 = Execute (F5)
database-execute-ctrl-enter = Execute (Ctrl+Enter)
database-fill-both-fields = Please fill in both fields.
database-error-title = Error
database-connection-success = Connection successful and saved!
database-connection-failed-title = Connection failed
database-psql-not-found = The 'psql' tool could not be found on this system.
database-unexpected-error = Unexpected error: { $error }
database-action-refused-connect = ❌ Action refused: switching database (\c) is not allowed here.
database-psql-not-found-console = ❌ Error: The 'psql' tool could not be found.
    Make sure PostgreSQL is installed on this PC and that 'psql.exe' is in the Windows PATH environment variable.
database-unexpected-error-console = ❌ Unexpected error: { $error }
database-wrong-password = The password is incorrect.
database-role-not-exist = The given username does not exist.
database-db-not-exist = The specified database could not be found.
database-connection-impossible = Connection impossible: check the Host (DB_HOST), the Port (DB_PORT), or whether the PostgreSQL server is running.
database-generic-connection-error = Could not connect with these parameters.

config-invalid-format = Invalid input format.
config-validation-error-title = Validation error
config-saved = Configuration saved!
config-null-value-must-be-none = The value of a null field must be 'None'.
config-bool-value-must-be-true-false = The value of a boolean field must be 'True' or 'False'.
config-cannot-convert = Could not convert '{ $value }' to { $type }.
config-unsupported-type = Unsupported type: { $type }
config-no-matching-union-type = The value '{ $value }' does not match any type in the union: [{ $types }].

secrets-title = Environment secrets
secrets-show-all = Show all
secrets-hide-all = Hide all
secrets-file-not-found = File not found or empty
secrets-value-empty = The value must not be empty!
secrets-saved = Secret saved!

language-toggle = FR/EN
