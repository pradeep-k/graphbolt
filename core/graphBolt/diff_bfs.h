#pragma once

#include <omp.h>
#include <algorithm>

#include "view_interface.h"
using std::min;

//typedef uint8_t level_t;
typedef vid_t level_t;


static level_t MAX_LEVEL = -2L;
static vid_t MAX_VID = -1L;

typedef bool (*reduce_fn_t)(level_t&, level_t level, int weight);
typedef bool (*reduce_pfn_t)(level_t*, level_t level, int weight);
typedef bool (*adjust_fn_t)(vid_t, level_t&, level_t level);

status_t create_adjacency_snapshot(ubatch_t* ubatch);

template <class T>
index_t stream_bfs_4(gview_t* viewh, Bitmap* lbitmap, Bitmap* rbitmap, vid_t osrc);

template <class T>
index_t stream_bfs_2a1(gview_t* viewh, Bitmap* lbitmap, Bitmap* rbitmap, vid_t osrc);

template <class T>
index_t stream_bfs_2b(gview_t* viewh, Bitmap* lbitmap, Bitmap* rbitmap, Bitmap* all_bmap, vid_t osrc); 

template <class T>
index_t stream_bfs_3(gview_t* viewh, Bitmap* all_bmap, vid_t osrc); 

inline bool bfs_op(level_t& status, level_t level, int weight) {
    if( status > level) {
        status = level;
        return true;
    }
    return false;
}

inline bool bfs_adjust(vid_t vid, level_t& status, level_t level) {
    if(status <= level ) {
        status = MAX_LEVEL;
        return true;
    }
    return false;
}

inline bool bfs_reduce(level_t& status, level_t level, int weight) {
    if(status > level +1) {
        status = level + 1;
        return true;
    }
    return false;
}

inline bool bfs_reduce_atomic(level_t* status, level_t level, int weight) {
    level_t old_level; 
    do {
        old_level = *status;
        if (old_level <= level + 1) return false;
    } while(!__sync_bool_compare_and_swap(status, old_level, level+1));

    return true;
}


struct bfs_info_t {
    vid_t*   parent;
    level_t* lstatus;
    level_t* rstatus;
    Bitmap*  lbitmap;
    Bitmap*  rbitmap;
    vid_t*   vids;
    reduce_fn_t op_fn;
    reduce_fn_t reduce_fn;
    reduce_pfn_t reduce_fn_atomic;
    adjust_fn_t adjust_fn;
    vid_t    root;
};

inline void print_bfs_summary(level_t* status, vid_t v_count)
{
    vid_t vid_count = 0;
    int l = 0;
    do {
        vid_count = 0;
        //#pragma omp parallel for reduction (+:vid_count) 
        for (vid_t v = 0; v < v_count; ++v) {
            if (status[v] == l) {
                ++vid_count;
                /*if (l == 0) {
                    cerr << v << endl;
                }*/
            }
        }
        cout << " Level = " << l << " count = " << vid_count << endl;
        ++l;
    } while (vid_count !=0);
}

bfs_info_t* init_bfs(gview_t* viewh, bool symm = true)
{
    vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)malloc(sizeof(bfs_info_t));
    
    vid_t* parent = (vid_t*)malloc(v_count*sizeof(vid_t));
    level_t* status = (level_t*)malloc(v_count*sizeof(level_t));
    
    memset(parent, 255, v_count*sizeof(vid_t));
    //memset(status, 255, v_count*sizeof(level_t));

    #pragma omp parallel for
    for (vid_t v = 0; v < v_count; ++v) {
        status[v] = MAX_LEVEL;
    }
    
    bfs_info->parent = parent;
    
    index_t meta = (index_t)viewh->get_algometa();
    bfs_info->root = _arg;
    status[bfs_info->root] = 0;
    
    bfs_info->lstatus = status;
    if (true == symm) {
        bfs_info->rstatus = bfs_info->lstatus;
    } else {
        bfs_info->rstatus = (level_t*)malloc(v_count*sizeof(level_t));
    }
    bfs_info->op_fn = bfs_op;
    bfs_info->reduce_fn = bfs_reduce;
    bfs_info->reduce_fn_atomic = bfs_reduce_atomic;
    bfs_info->adjust_fn = bfs_adjust;
    viewh->set_algometa(bfs_info);
    cout << "root:" << bfs_info->root << endl;
    return bfs_info;
}

//Only for sstream based view: Ddir
template <class T> 
index_t stream_bfs_1ddir(gview_t* viewh, Bitmap* rbitmap, Bitmap* all_bmap, vid_t osrc)
{
    //vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    //level_t* lstatus = bfs_info->lstatus;
    level_t* rstatus = bfs_info->rstatus;
    index_t frontiers = 0;
    
    edgeT_t<T>* edges = 0;
    index_t edge_count = viewh->get_new_edges(edges);
    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
    {
    vid_t parent;
    vid_t nebr;
    vid_t v;
    bool is_del = false;

    //1
    #pragma omp for schedule (dynamic, 1024) nowait
    for (index_t e = 0; e < edge_count; ++e) {
        nebr = TO_VID(get_src(edges[e]));
        v =  TO_VID(get_dst(edges+e));
        is_del = IS_DEL(get_src(edges[e]));

        if ((false == is_del) || (rstatus[v] == MAX_LEVEL)) continue;
            
        parent = bfs_info->parent[v];
        if (nebr + osrc == parent) {
            rbitmap->set_bit_atomic(v);
            ++frontiers;
            all_bmap->set_bit_atomic(v);
        }
    }
    }
    return frontiers;
}

//Only for sstream based views: Udir
template <class T>
index_t stream_bfs_1udir(gview_t* viewh, Bitmap* rbitmap, Bitmap* all_bmap) 
{
    //vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    //level_t* lstatus = bfs_info->lstatus;
    level_t* rstatus = bfs_info->rstatus;
    index_t frontiers = 0;

    Bitmap* bitmap_out = viewh->bitmap_out;
    
    edgeT_t<T>* edges = 0;
    index_t edge_count = viewh->get_new_edges(edges);
    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
    {
    vid_t parent;
    vid_t nebr;
    vid_t v;
    bool is_del = false;

    //1
    #pragma omp for schedule (dynamic, 1024) nowait
    for (index_t e = 0; e < edge_count; ++e) {
        nebr = TO_VID(get_src(edges[e]));
        v =  TO_VID(get_dst(edges+e));
        is_del = IS_DEL(get_src(edges[e]));

        if ((false == is_del) || (rstatus[v] == MAX_LEVEL)) continue;
            
        parent = bfs_info->parent[v];
        if (nebr == parent) {
            rbitmap->set_bit_atomic(v);
            ++frontiers;
            all_bmap->set_bit_atomic(v);
        }
        parent = bfs_info->parent[nebr];
        if (v == parent) {
            //even though we are doing opposite of above, we want to set the rbitmap only
            rbitmap->set_bit_atomic(nebr);
            ++frontiers;
            all_bmap->set_bit_atomic(nebr);
        }
    }
    }
    return frontiers;
}

//Only for sstream based views: Udir
template <class T>
index_t stream_bfs_5udir(gview_t* viewh) 
{
    vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    level_t* lstatus = bfs_info->lstatus;
    level_t* rstatus = bfs_info->rstatus;
    index_t frontiers = 0;
    
    edgeT_t<T>* edges = 0;
    index_t edge_count = viewh->get_new_edges(edges);
    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
    {
    vid_t parent;
    vid_t src;
    vid_t dst;
    level_t src_level, dst_level;
    bool is_del = false;

    //1
    #pragma omp for schedule (dynamic, 1024) nowait
    for (index_t e = 0; e < edge_count; ++e) {
        src = TO_VID(get_src(edges[e]));
        dst =  TO_VID(get_dst(edges+e));
        is_del = IS_DEL(get_src(edges[e]));
        src_level = rstatus[src];
        dst_level = rstatus[dst];
        if (false == is_del) {
            if (dst_level > src_level + 1) {
                rstatus[dst] = src_level + 1;
                bfs_info->parent[dst] = src;
                viewh->bitmap_out->set_bit_atomic(dst);
            } else if (dst_level + 1 < src_level) {
                rstatus[src] = dst_level + 1;
                bfs_info->parent[src] = dst;
                viewh->bitmap_out->set_bit_atomic(src);
            }
        }
    }
    }
    return 0;
}

//Only for sstream based views: ddir
template <class T>
index_t stream_bfs_5ddir(gview_t* viewh) 
{
    vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    level_t* lstatus = bfs_info->lstatus;
    level_t* rstatus = bfs_info->rstatus;
    index_t frontiers = 0;
    
    edgeT_t<T>* edges = 0;
    index_t edge_count = viewh->get_new_edges(edges);
    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
    {
    vid_t parent;
    vid_t src;
    vid_t dst;
    level_t src_level, dst_level;
    bool is_del = false;

    //1
    #pragma omp for schedule (dynamic, 1024) nowait
    for (index_t e = 0; e < edge_count; ++e) {
        src = TO_VID(get_src(edges[e]));
        dst =  TO_VID(get_dst(edges+e));
        is_del = IS_DEL(get_src(edges[e]));
        src_level = rstatus[src];
        dst_level = rstatus[dst];
        if ((false == is_del) && (dst_level > src_level + 1)) {
            rstatus[dst] = src_level + 1;
            bfs_info->parent[dst] = src;
            viewh->bitmap_out->set_bit_atomic(dst);
        }
    }
    }
    return 0;
}

template <class T> 
void do_streambfs1(gview_t* viewh, Bitmap* bmap_out, Bitmap* bmap_in, Bitmap* all_bmap)
{
    vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    level_t* rstatus = bfs_info->rstatus;
    //level_t* lstatus = bfs_info->lstatus;

    //Bitmap* bitmap_in = viewh->bitmap_in;

    all_bmap->reset();
    index_t frontiers = 0;
    index_t total = 0;

    double start = mywtime();
    if (viewh->is_ddir()) {
        frontiers = stream_bfs_1ddir<T>(viewh, bmap_in, all_bmap, 0);
    } else {
        frontiers = stream_bfs_1udir<T>(viewh, bmap_in, all_bmap);
    }
    total += frontiers;
    double end1 = mywtime();
    while (frontiers) {//
        frontiers = stream_bfs_2a1<T>(viewh, bmap_out, bmap_in, 0);

        //bmap_in->reset();
        if (frontiers == 0) break;
        frontiers = 0;
        //double end2 = mywtime();

        //2
        frontiers = stream_bfs_2b<T>(viewh, bmap_out, bmap_in, all_bmap, 0);
        total += frontiers;
        //bmap_out->reset();
    }

    double end2 = mywtime();
    frontiers = 0;
    if (total) {
        //3. Pull once more for all affected vertex
        stream_bfs_3<T>(viewh, all_bmap, 0);
    }

    double end3 = mywtime();
    //5.
    if (viewh->is_ddir()) {
        stream_bfs_5ddir<T>(viewh);
    } else {
        stream_bfs_5udir<T>(viewh);
    }
    //4.
    //switch to traditional incremental version
    Bitmap* lbitmap = all_bmap;//viewh->bitmap_out;
    lbitmap->do_or(viewh->bitmap_out);
    Bitmap* rbitmap = bmap_in;
    //rbitmap->reset();
    
    do {
        frontiers = stream_bfs_4<T>(viewh, lbitmap, rbitmap, 0);
        //lbitmap->reset();
        std::swap(lbitmap, rbitmap);
    } while (frontiers);
    double end4 = mywtime();

    cout << end1 - start << ":" << end2 - end1 << ":" << end3 - end2 << ":" << end4 - end3 << "::" << end4 - start << endl;
}

//pull bfs for vertices which has deleted parents
//mark its children so that they can process them
template <class T>
index_t stream_bfs_2a1(gview_t* viewh, Bitmap* lbitmap, Bitmap* rbitmap, vid_t osrc) 
{
    vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    level_t* lstatus = bfs_info->lstatus;
    level_t* rstatus = bfs_info->rstatus;
    index_t frontiers = 0;
    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
    {
        nebr_reader_t adj_list;
        int weight = 1;
        int w = 0;
        degree_t adj_count = 0;
        vid_t new_parent = MAX_VID;
        vid_t nebr;
        level_t nebr_level = MAX_LEVEL;
        //1b
        #pragma omp for schedule (dynamic, 1024) nowait
        for (vid_t v = 0; v < v_count; ++v) {
            if (!rbitmap->get_bit(v)) continue;
            rbitmap->reset_bit(v);

            adj_count = viewh->get_nebrs_in(v, adj_list);
            nebr_level = MAX_LEVEL;
            for (degree_t i = 0; i < adj_count; ++i) {
                nebr = TO_VID(adj_list.get_sid(i));
                w = get_weight_int(adj_list.get_item<T>(i)); 
                if ((v+osrc != bfs_info->parent[nebr]) && bfs_info->op_fn(nebr_level, lstatus[nebr], w)) {
                    new_parent = nebr;
                    weight = get_weight_int(adj_list.get_item<T>(i)); 
                }
            }
            //== level+1, update parent, do nothing
            //< level+1, update parent, because of added edge, will be taken care later
            /*vid_t parent = bfs_info->parent[v];
            if (nebr_level == rstatus[v] && v != new_parent && parent != new_parent) { //traverse its children
                rstatus[v]= nebr_level+1;
                bfs_info->parent[v] = new_parent + osrc;
                lbitmap->set_bit(v);
                ++frontiers;
            } else */
            if (bfs_info->adjust_fn(v, rstatus[v], nebr_level)) { //traverse its children
                bfs_info->parent[v] = MAX_VID;
                lbitmap->set_bit(v);
                ++frontiers;
            } else {
                bfs_info->reduce_fn(rstatus[v], nebr_level, weight);
                bfs_info->parent[v] = new_parent + osrc;
            }
        }
    }
    return frontiers;
}

//simply mark the children of a vertex so that can seek thier new parent
template <class T>
index_t stream_bfs_2b(gview_t* viewh, Bitmap* lbitmap, Bitmap* rbitmap, Bitmap* all_bmap, vid_t osrc) 
{
    vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    //level_t* lstatus = bfs_info->lstatus;
    //level_t* rstatus = bfs_info->rstatus;
    index_t frontiers = 0;
    //2
    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
    {
        nebr_reader_t adj_list;
        degree_t adj_count = 0;
        vid_t nebr;
        #pragma omp for schedule (dynamic, 1024) nowait
        for (vid_t v = 0; v < v_count; ++v) {
            if (!lbitmap->get_bit(v)) continue;
            lbitmap->reset_bit(v);
            adj_count = viewh->get_nebrs_out(v, adj_list);
            for (degree_t i = 0; i < adj_count; ++i) {
                nebr = TO_VID(adj_list.get_sid(i));
                if (bfs_info->parent[nebr] == v+osrc) {
                    //mark nebr for 1b processing
                    rbitmap->set_bit_atomic(nebr);
                    ++frontiers;
                    all_bmap->set_bit_atomic(nebr);
                }
            }
        }
    }
    return frontiers;
}

template <class T>
index_t stream_bfs_3(gview_t* viewh, Bitmap* all_bmap, vid_t osrc) 
{
    vid_t v_count = viewh->get_vcount();
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    level_t* lstatus = bfs_info->lstatus;
    level_t* rstatus = bfs_info->rstatus;
    index_t frontiers = 0;
    //3. Pull once more for all affected vertex
    #pragma omp parallel num_threads(THD_COUNT) reduction(+:frontiers)
    {
        nebr_reader_t adj_list;
        int weight = 1;
        int w = 0;
        degree_t adj_count = 0;
        vid_t new_parent = MAX_VID;
        vid_t nebr;
        level_t nebr_level = MAX_LEVEL;
        #pragma omp for schedule (dynamic, 1024) nowait
        for (vid_t v = 0; v < v_count; ++v) {
            if (!all_bmap->get_bit(v)) continue;
            adj_count = viewh->get_nebrs_in(v, adj_list);
            nebr_level = MAX_LEVEL;
            for (degree_t i = 0; i < adj_count; ++i) {
                nebr = TO_VID(adj_list.get_sid(i));
                w = get_weight_int(adj_list.get_item<T>(i)); 
                if (bfs_info->op_fn(nebr_level, lstatus[nebr], w)) {
                    new_parent = nebr;
                    weight = get_weight_int(adj_list.get_item<T>(i)); 
                }
            }
            if (nebr_level != MAX_LEVEL && bfs_info->reduce_fn(rstatus[v], nebr_level, weight)) {
                bfs_info->parent[v] = new_parent + osrc;
                frontiers++;
            }
        }
    }
    return frontiers;
}
template <class T>
index_t stream_bfs_4(gview_t* viewh, Bitmap* lbitmap, Bitmap* rbitmap, vid_t osrc) 
{
    vid_t v_count = viewh->get_vcount();
    //comm2d_t* comm = viewh;
    //vid_t v_offset = 0; //comm->row_rank*(v_count);
    bfs_info_t* bfs_info = (bfs_info_t*)viewh->get_algometa();
    level_t* lstatus = bfs_info->lstatus;
    level_t* rstatus = bfs_info->rstatus;
    index_t frontiers = 0;
    #pragma omp parallel num_threads (THD_COUNT) reduction(+:frontiers)
    {
        int weight = 1;
        sid_t sid;
        level_t level = 0;
        degree_t nebr_count = 0;
        nebr_reader_t local_adjlist;
        T* dst;

        #pragma omp for schedule (dynamic, 1024) nowait
        for (vid_t v = 0; v < v_count; v++) {
            if(false == lbitmap->get_bit(v)) continue;
            lbitmap->reset_bit(v);
            level = lstatus[v];
            if (level == MAX_LEVEL) continue;

            nebr_count = viewh->get_nebrs_out(v, local_adjlist);
            for (degree_t i = 0; i < nebr_count; ++i) {
                dst = local_adjlist.get_item<T>(i);
                sid = TO_VID(get_sid(*dst));
                weight = get_weight_int(dst); 

                if (bfs_info->reduce_fn_atomic(rstatus+sid, level, weight)) {
                    bfs_info->parent[sid] = v + osrc; 
                    rbitmap->set_bit_atomic(sid);
                    ++frontiers;
                    //cerr << v+osrc << ":" << sid+v_offset << endl;
                }
            }
        }
    }
    return frontiers;
}

template <class T>
void* stream_bfs_del(void* viewh)
{
    gview_t* sstreamh = (gview_t*)(viewh);
    int update_count = 0;
    
    vid_t v_count = sstreamh->get_vcount();
   
    bfs_info_t* bfs_info = init_bfs(sstreamh);
    Bitmap bmap_in1(v_count);
    Bitmap bmap_out1(v_count);
    Bitmap all_bmap1(v_count);

    
    double end = 0, end1 = 0;;
    double start = mywtime ();
    while (eEndBatch != sstreamh->update_view()) {
        end = mywtime();
        do_streambfs1<T>(sstreamh, &bmap_out1, &bmap_in1, &all_bmap1);
        end1 = mywtime();
        cout << sstreamh->update_count <<" " << end - start << "::" << end1 - end << endl << endl;
        start = mywtime();
    }
    print_bfs_summary(bfs_info->lstatus, v_count);
    cout << " update_count = " << sstreamh->update_count << endl;
         //<< " snapshot count = " << sstreamh->get_snapid() << endl;
    return 0;
}

template<class T>
void* stream_serial_bfs_del(void* viewh)
{
    gview_t* sstreamh = (gview_t*)(viewh);
    vid_t v_count = sstreamh->get_vcount();
    //pgraph_t* pgraph  = sstreamh->pgraph;
    
    Bitmap bmap_in1(v_count);
    Bitmap bmap_out1(v_count);
    Bitmap all_bmap1(v_count);
    
    index_t batch_size = residue;
    index_t marker = 0; 

    //cout << "starting BFS" << endl;
    bfs_info_t* bfs_info = init_bfs(sstreamh);

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
	    
        do_streambfs1<T>(sstreamh, &bmap_out1, &bmap_in1, &all_bmap1);
        //print_bfs_summary(bfs_info->lstatus, v_count);
        end2 = mywtime();
        
        //cout << "BFS Time at Batch " << update_count << " = " << end - start << endl;
        cout << update_count
             << ":" << sstreamh->get_snapmarker()
             << ":" << end - start << ":" << end1 - end  
             << ":" << end2 - end1 << endl << endl;
    } 
    double endn = mywtime();
    print_bfs_summary(bfs_info->lstatus, v_count);

    cout << "update_count = " << update_count << "Time = "<< endn - startn << endl;
    return 0;
}
