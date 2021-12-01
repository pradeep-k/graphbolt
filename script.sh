time ./apps/BFS -source 0 -numberOfUpdateBatches 512 -nEdges 65536 -streamPath ~/data/kron-21/onefile/k21.txt ~/data/kron-21/k21_empty.txt > res.txt
cat res.txt | grep -i 'Total Ingestion time' | cut -f5 -d" "   | paste -sd+ | bc
