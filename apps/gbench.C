#include <algorithm>

#include "../core/common/utils.h"
#include "../core/graph/IO.h"
#include "../core/graph/graph.h"
#include "../core/graph/vertex.h"
#include "../core/graphBolt/ingestor.h"
#include "../core/graphBolt/new_type.h"
#include "../core/graphBolt/batching.h"
#include "../core/graphBolt/util.h"
#include "../core/graphBolt/graph_view.h"
#include "../core/graphBolt/diff_bfs.h"

using namespace std;

typekv_t* typekv;
void* gb_ingestor = 0;
//Ingestor<asymmetricVertex>* gb_ingestor;
ubatch_t* ubatch = 0;

int _part_count;
index_t residue = 0;
int THD_COUNT = 9;
vid_t _global_vcount = 0;
int _dir = 0;//undirected
int _source = 0;//text
int _num_sources = 1;//only one data-source
int _format = 0;
int64_t _arg = 1;//algo specific argument
int _numtasks = 0, _rank = 0;
int _numlogs = 1;

index_t  BATCH_SIZE = (1L << 16);//edge batching in edge log
index_t  BATCH_MASK =  0xFFFF;
index_t  BATCH_TIMEOUT = (-1L);

#ifdef B64
propid_t INVALID_PID = 0xFFFF;
tid_t    INVALID_TID  = 0xFFFFFFFF;
sid_t    INVALID_SID  = 0xFFFFFFFFFFFFFFFF;
#else 
propid_t INVALID_PID = 0xFF;
tid_t    INVALID_TID  = 0xFF;
sid_t    INVALID_SID  = 0xFFFFFFFF;
#endif


//run adjacency store creation here.
status_t create_adjacency_snapshot(ubatch_t* ubatch)
{
    if (_dir == 1) {//directed
        Ingestor<asymmetricVertex>* ingestor = 
        (Ingestor<asymmetricVertex>*) gb_ingestor;
        ingestor->processNextBatch();
    } else if (_dir ==0)  {//undirected
        Ingestor<symmetricVertex>* ingestor = 
        (Ingestor<symmetricVertex>*) gb_ingestor;
        ingestor->processNextBatch();
    }
    return eOK;
}

int main(int argc, char** argv)
{
    //In the processNextBatch call _gbench version of getNewEdgesFromFile.
    commandLine P(argc, argv);
    char *iFile = P.getArgument(0);
    bool symmetric = P.getOptionValue("-s");
    
    string idir = P.getOptionValue("-streamPath", "./data/");
    size_t batch_size = P.getOptionLongValue("-nEdges", 65536);

    // bool useExtraBufferSpace = P.getOptionValue("-buffer");
    bool simpleFlag = P.getOptionValue("-simple");
    bool debugFlag = P.getOptionValue("-debug");

    int n_workers = P.getOptionIntValue("-nWorkers", getWorkers());
    setCustomWorkers(n_workers);
    THD_COUNT = n_workers; 

    cout << fixed;

    //Return ubatch pointer here.
    ubatch = new ubatch_t(sizeof(edge_t),  1);
    ubatch->alloc_edgelog(20); // 1 Million edges
    ubatch->reg_archiving();

    
    //create typekv as it handles the mapping for vertex id (string to vid_t)
    typekv = new typekv_t;
    gview_t* sstreamh = 0;
    
    //Initialize the system. 

    if (symmetric) {
        // symmetric graph,un directed
        _dir = 0;
        graph<symmetricVertex> 
        G = readGraph<symmetricVertex>(iFile, symmetric, simpleFlag, debugFlag);
        G.setSymmetric(true);
        vid_t v_count = G.n;
        typekv->manual_setup(v_count, true, "gtype");

        //ingestor =  new Ingestor(G, P);
        Ingestor<symmetricVertex> ingestor(G, P);
        gb_ingestor = &ingestor;
        
        //If system support adjacency store snapshot, create thread for index creation
        // Run analytics in separte thread. If adjacency store is non-snapshot, do indexing and analytics in seq.
        index_t slide_sz = (1L << 16);
        sstreamh = reg_sstream_view<symmetricVertex>(&G, ubatch, v_count, kickstarter_bfs_serial<dst_id_t>, C_THREAD, slide_sz);
    
        //perform micro batching here using ubatch pointer
        int64_t flags = SOURCE_BINARY;
        index_t total_edges = add_edges_from_dir<dst_id_t>(idir, ubatch, flags);
        ubatch->set_total_edges(total_edges);
        
        //Wait for threads to complete
        void* ret;
        pthread_join(sstreamh->thread, &ret);
    } else {
        // asymmetric graph, directed
        _dir = 1;
        graph<asymmetricVertex> 
        G = readGraph<asymmetricVertex>(iFile, symmetric, simpleFlag, debugFlag);
        cout << "Graph created" << endl;
        vid_t v_count = G.n;
        typekv->manual_setup(v_count, true, "gtype");

        //ingestor =  new Ingestor(G, P);
        Ingestor<asymmetricVertex> ingestor(G, P);
        gb_ingestor = &ingestor;
        
        //If system support adjacency store snapshot, create thread for index creation
        // Run analytics in separte thread. If adjacency store is non-snapshot, do indexing and analytics in seq.
        index_t slide_sz = (1L << 16);
        sstreamh = reg_sstream_view<asymmetricVertex>(&G, ubatch, v_count, kickstarter_bfs_serial<dst_id_t>, C_THREAD, slide_sz);
    
        //perform micro batching here using ubatch pointer
        int64_t flags = SOURCE_BINARY;
        index_t total_edges = add_edges_from_dir<dst_id_t>(idir, ubatch, flags);
        ubatch->set_total_edges(total_edges);
        
        //Wait for threads to complete
        void* ret;
        pthread_join(sstreamh->thread, &ret);
    
    }

    return 0;

    //compute(G, P);
    //G.del();
}
