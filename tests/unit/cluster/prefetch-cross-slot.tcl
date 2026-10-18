# Exercise queued commands with both main-thread and I/O-thread parsing.
foreach threads {1 4} {
    start_cluster 1 0 [list tags {external:skip cluster} overrides [list io-threads $threads prefetch-batch-max-size 16]] {
        test "Queued CROSSSLOT commands preserve following replies with [lindex [R 0 config get io-threads] 1] I/O threads" {
            R 0 config set io-threads-always-active yes
            R 0 set "{prefetch}a" first
            R 0 set "{prefetch}b" second
            assert_not_equal [R 0 cluster keyslot "{prefetch}a"] [R 0 cluster keyslot "{other}a"]
            set rd [valkey_deferring_client]
            # One write puts invalid commands and valid commands in the same queue.
            set pipeline ""
            foreach command [list [list mget "{prefetch}a" "{other}a"] [list copy "{prefetch}a" "{other}a"] [list mget "{prefetch}a" "{prefetch}b"] [list copy "{prefetch}a" "{prefetch}c"] [list get "{prefetch}c"] [list ping]] {
                append pipeline "*[llength $command]\r\n"
                foreach arg $command { append pipeline "\$[string length $arg]\r\n$arg\r\n" }
            }
            for {set i 0} {$i < 100} {incr i} {
                R 0 del "{prefetch}c"
                $rd write $pipeline
                $rd flush
                assert_error "CROSSSLOT*" {$rd read}
                assert_error "CROSSSLOT*" {$rd read}
                assert_equal {first second} [$rd read]
                assert_equal 1 [$rd read]
                assert_equal first [$rd read]
                assert_equal PONG [$rd read]
            }
            assert_equal PONG [R 0 ping]
            $rd close
        }
    }
}
