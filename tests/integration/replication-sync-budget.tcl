start_server {tags {repl external:skip} overrides {save {}}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary config set repl-diskless-sync-delay 0
    $primary config set repl-ping-replica-period 3600
    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica replicaof $primary_host $primary_port
        wait_for_condition 50 100 {
            [s master_link_status] eq {up}
        } else {
            fail "Replication did not start"
        }
        test "Offset synchronization permits an explicit catch-up budget" {
            $primary set before value
            wait_for_ofs_sync $primary $replica
            # Keep INFO responsive while applying writes takes longer than the old five-second helper budget.
            $replica client pause 6000 write
            $primary set delayed value
            wait_for_ofs_sync $primary $replica 500
            assert_equal value [$replica get delayed]
        }
        test "Offset synchronization accepts replica-first callers" {
            wait_for_ofs_sync $primary $replica
            $replica client pause 1000 write
            $primary set reversed value
            wait_for_ofs_sync $replica $primary
            assert_equal value [$replica get reversed]
        }
    }
}
