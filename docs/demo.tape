# Demo GIF for the README. Render with https://github.com/charmbracelet/vhs
# (and jq):
#
#   docker compose down -v && docker compose up -d --build && make build
#   vhs docs/demo.tape            # writes docs/demo.gif
#   docker compose down -v
#
# Needs a fresh compose cluster (the demo `orders` table empty) and
# bin/curntvalv. Typing is shown; waits are hidden with Sleep.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 15
Set Width 1200
Set Height 760
Set Padding 16
Set TypingSpeed 30ms
Set Theme "Builtin Dark"

Hide
Type "export PATH=$PWD/bin:$PATH PS1='$ '" Enter
Type `psql() { docker compose exec -T postgres psql -U postgres -qc "$1"; }` Enter
Type `ch() { docker compose exec -T clickhouse clickhouse-client --password curntvalv -q "$1"; }` Enter
Type "clear" Enter
Show

Type "# Three curntValv nodes read Postgres and deliver to ClickHouse" Enter
Type "curntvalv status" Enter
Sleep 3s

Type "# One transaction, two rows" Enter
Type `psql "INSERT INTO orders (item, qty) VALUES ('apple', 3), ('pear', 1)"` Enter
Sleep 1s
Type `curntvalv subscribe --table orders --transactions --limit 2 | jq -c '{op, after, tx_changes}'` Enter
Sleep 3s

Type "# Kill the leader, keep writing" Enter
Type `docker compose kill -s KILL $(curntvalv status | awk '$3 == "leader" {print "node" $2}')` Enter
Sleep 4s
Type `psql "UPDATE orders SET qty = 5 WHERE item = 'apple'; DELETE FROM orders WHERE item = 'pear'"` Enter
Sleep 2s
Type "curntvalv status" Enter
Sleep 4s

Type "# ClickHouse mirrors the table, nothing lost or doubled" Enter
Type `ch "SELECT id, item, qty FROM public_orders FINAL"` Enter
Sleep 4s
