(time ./gbench -source 1  -batch-size 12 -nEdges 4096 -streamPath ~/data/kron-21/mix_bins/ -nWorkers 19 -s 1 ~/data/kron-21/adj.txt) > k21_del_u16.txt 2>&1
sleep 10
(time ./gbench -source 1  -batch-size 12 -nEdges 4096 -streamPath ~/data/twitter_mpi/mix_bins/ -nWorkers 19  -s 1 ~/data/twitter_mpi/adj.txt) > tw_del_u16.txt 2>&1
sleep 10
(time ./gbench -source 1  -batch-size 12 -nEdges 4096 -streamPath ~/data/friendster/mix_bins/ -nWorkers 19 -s 1  ~/data/friendster/adj.txt) > fr_del_u16.txt 2>&1
sleep 10
(time ./gbench -source 0  -batch-size 12 -nEdges 4096 -streamPath ~/data/subdomain/mix_bins/ -nWorkers 19 -s 1 ~/data/subdomain/adj.txt) > sb_del_u16.txt 2>&1
#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/kron-21/bin/ -nWorkers 19 -s 1 ~/data/kron-21/adj.txt) > k21_u_ingestion.txt 2>&1
#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/twitter_mpi/shuffled/ -nWorkers 19  -s 1 ~/data/twitter_mpi/adj.txt) > tw_u_nodel.txt 2>&1
#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/friendster/shuffled/ -nWorkers 19 -s 1  ~/data/friendster/adj.txt) > fr_u_nodel.txt 2>&1
#sleep 10
#(time ./gbench -source 0  -batch-size 16 -nEdges 65536 -streamPath ~/data/subdomain/shuffled/ -nWorkers 19 -s 1 ~/data/subdomain/adj.txt) > sb_u.txt 2>&1
#
#

#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/kron-21/mix_bins/ -nWorkers 19 ~/data/kron-21/adj.txt) > k21_del_d_ingestion.txt 2>&1
#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/twitter_mpi/mix_bins/ -nWorkers 19 ~/data/twitter_mpi/adj.txt) > tw_del_d_ingestion.txt 2>&1
#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/friendster/mix_bins/ -nWorkers 19 ~/data/friendster/adj.txt) > fr_del_d_ingestion.txt 2>&1
#sleep 10
#(time ./gbench -source 0  -batch-size 16 -nEdges 65536 -streamPath ~/data/subdomain/mix_bins/ -nWorkers 19 ~/data/subdomain/adj.txt) > sb_del_d.txt 2>&1
#


#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/kron-21/bin/ -nWorkers 19 ~/data/kron-21/adj.txt) > k21_d_ingestion.txt 2>&1
#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/twitter_mpi/shuffled/ -nWorkers 19 ~/data/twitter_mpi/adj.txt) > tw_d_nodel.txt 2>&1
#sleep 10
#(time ./gbench -source 1  -batch-size 16 -nEdges 65536 -streamPath ~/data/friendster/shuffled/ -nWorkers 19 ~/data/friendster/adj.txt) > fr_d_nodel.txt 2>&1
#sleep 10
#(time ./gbench -source 0  -batch-size 16 -nEdges 65536 -streamPath ~/data/subdomain/shuffled/ -nWorkers 19 ~/data/subdomain/adj.txt) > sb_d.txt 2>&1
