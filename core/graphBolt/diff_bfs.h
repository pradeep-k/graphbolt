#pragma once

#include "graph_view.h"
#include "engine.h"
#include "bfs_info.h"

status_t create_adjacency_snapshot(ubatch_t* ubatch);

template <class T>
void* kickstarter_bfs(void* viewh)
{
    gview_t* sstreamh = (gview_t*)(viewh);
    int update_count = 0;
    vid_t v_count = sstreamh->get_vcount();
    
    BfsInfo global_info(_arg);
    KickStarterEngine<T, uint16_t, BfsInfo> engine(sstreamh, global_info);
    engine.init();
    engine.initialCompute(); 
    double end = 0, end1 = 0;;
    double start = mywtime ();
    while (eEndBatch != sstreamh->update_view()) {
        end = mywtime();
        engine.deltaCompute();
        end1 = mywtime();
        cout << sstreamh->update_count <<" " << end - start << "::" << end1 - end << endl;
        //engine.printOutput();
        start = mywtime();
    }
    engine.printOutput();
    cout << " update_count = " << sstreamh->update_count << endl; 
    return 0;
}
template<class T>
void* kickstarter_bfs_serial(void* viewh)
{
    gview_t* sstreamh = (gview_t*)(viewh);
    vid_t v_count = sstreamh->get_vcount();
    
    BfsInfo global_info(_arg);
    KickStarterEngine<T, uint16_t, BfsInfo> engine(sstreamh, global_info);
    engine.init();
    engine.initialCompute(); 
    
    index_t batch_size = residue;
    index_t marker = 0; 

    int update_count = 0;
    status_t ret = eOK; 
    double start, end, end1, end2;
    
    double startn = mywtime();
    while (true) {
        start = mywtime();
        create_adjacency_snapshot(sstreamh->ubatch);
        end = mywtime();
        
        //update the sstream view
        ret = sstreamh->update_view();
        end1 = mywtime();
        if (ret == eEndBatch) break;
        ++update_count;
	    
        engine.deltaCompute();
        end2 = mywtime();
        
        //cout << "BFS Time at Batch " << update_count << " = " << end - start << endl;
        cout << update_count
             << ":" << sstreamh->get_snapmarker()
             << ":" << end - start << ":" << end1 - end  
             << ":" << end2 - end1 << endl;
    } 
    double endn = mywtime();
    engine.printOutput();
    cout << "update_count = " << update_count << "Time = "<< endn - startn << endl;
    return 0;
}
