app-title = NoPubSearch — Crawler

tabs-dashboard = Dashboard
tabs-logs = Logs
tabs-database-console = Console Base de Données
tabs-configs = Configs
tabs-secrets-configs = Secrets Configs

dashboard-domains-visited = Domaines visités
dashboard-pages-crawled = Pages crawlées
dashboard-pages-per-minute = Pages / min
dashboard-urls-waiting = URLs en attente
dashboard-recent-errors = Erreurs récentes
dashboard-bloom-filter = URLs dans le bloom filter
dashboard-uptime = Uptime
dashboard-all-threads = Tous les threads
dashboard-pause = ⏸ Pause
dashboard-resume = ▶ Reprendre
dashboard-stop = ⏹ Stop
dashboard-threads-running = Threads en cours

thread-running = ● En cours
thread-paused = ● En pause
thread-stopped = ● Arrêté
thread-pages-count = { $count ->
    [0] Aucune page
    [one] { $count } page
   *[other] { $count } pages
}
thread-active-seconds-ago = actif il y a { $seconds }s
thread-active-minutes-ago = actif il y a { $minutes }min
thread-pause = Pause
thread-resume = Reprendre
thread-stop = Stop

logs-select-source = Sélectionnez une source de logs
logs-status-bar = Source : { $crawler }  ·  Niveau min. : { $level }
logs-back = Retour
logs-level-up = Niveau Sup. (+)
logs-level-down = Niveau Inf. (-)
logs-all-crawlers = Tous les crawlers

database-admin-credentials-title = 🔒 Identifiants d'Administration PostgreSQL
database-admin-username-placeholder = Nom d'utilisateur admin (ex: postgres)
database-password-placeholder = Mot de passe
database-save-and-access = Sauvegarder et Accéder
database-testing-connection = Test de connexion en cours...
database-console-title = Console PostgreSQL (Base restreinte : { $db_name })
database-edit-credentials = Modifier les identifiants
database-execute-f5 = Exécuter (F5)
database-execute-ctrl-enter = Exécuter (Ctrl+Enter)
database-fill-both-fields = Veuillez remplir les deux champs.
database-error-title = Erreur
database-connection-success = Connexion réussie et sauvegardée !
database-connection-failed-title = Échec de connexion
database-psql-not-found = L'outil 'psql' est introuvable sur le système.
database-unexpected-error = Erreur inattendue : { $error }
database-action-refused-connect = ❌ Action refusée : Le changement de base de données (\c) n'est pas autorisé ici.
database-psql-not-found-console = ❌ Erreur : L'outil 'psql' est introuvable.
    Vérifiez que PostgreSQL est bien installé sur ce PC et que 'psql.exe' est dans les variables d'environnement (PATH) de Windows.
database-unexpected-error-console = ❌ Erreur inattendue : { $error }
database-wrong-password = Le mot de passe est incorrect.
database-role-not-exist = Le nom d'utilisateur saisi n'existe pas.
database-db-not-exist = La base de données spécifiée est introuvable.
database-connection-impossible = Connexion impossible : Vérifiez l'Hôte (DB_HOST), le Port (DB_PORT) ou si le serveur PostgreSQL est actif.
database-generic-connection-error = Impossible de se connecter avec ces paramètres.

config-invalid-format = Format de saisie invalide.
config-validation-error-title = Erreur de validation
config-saved = Configuration enregistrée !
config-null-value-must-be-none = La valeur d'un champ nul doit être 'None'.
config-bool-value-must-be-true-false = La valeur d'un champ booléen doit être 'True' ou 'False'.
config-cannot-convert = Impossible de convertir '{ $value }' en { $type }.
config-unsupported-type = Type non supporté : { $type }
config-no-matching-union-type = La valeur '{ $value }' ne correspond à aucun type de l'union : [{ $types }].

secrets-title = Secrets d'environnement
secrets-show-all = Afficher tout
secrets-hide-all = Masquer tout
secrets-file-not-found = Fichier introuvable ou vide
secrets-value-empty = La valeur ne doit pas être vide !
secrets-saved = Secret enregistré !

language-toggle = FR/EN
