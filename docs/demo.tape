# Demo GIF for the README. Render with https://github.com/charmbracelet/vhs:
#
#   docker compose up -d --build && make build
#   vhs docs/demo.tape            # writes docs/demo.gif
#   docker compose down -v
#
# Needs a fresh compose cluster (the demo `orders` table empty) and
# bin/curntvalv. Typing is shown; waits are hidden with Sleep.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 15
Set Width 1200
Set Height 720
Set Padding 16
Set TypingSpeed 35ms
Set Theme "Builtin Dark"

Hide
Type "export PATH=$PWD/bin:$PATH PS1='$ '" Enter
Type "psql() { docker compose exec -T postgres psql -U postgres -qc \"$1\"; }" Enter
Type "clear" Enter
Show

Type "# A 3-node curntValv cluster reading Postgres, delivering to ClickHouse" Enter
Type "curntvalv status" Enter
Sleep 3s

Type "psql \"INSERT INTO orders (item, qty) VALUES ('apple', 3), ('pear', 1)\"" Enter
Sleep 1s
Type "curntvalv subscribe --table orders" Enter
Sleep 3s
Ctrl+C
Sleep 500ms

Type "# Kill the leader" Enter
Type "docker compose kill $(curntvalv status | awk '$3 == \"leader\" {print \"node\" $2}')" Enter
Sleep 4s
Type "psql \"UPDATE orders SET qty = 5 WHERE item = 'apple'\"" Enter
Sleep 2s
Type "curntvalv status" Enter
Sleep 3s

Type "# Every change is in ClickHouse, once" Enter
Type "docker compose exec clickhouse clickhouse-client --password curntvalv -q 'SELECT op, after FROM curntvalv_events FINAL ORDER BY log_index'" Enter
Sleep 4s
