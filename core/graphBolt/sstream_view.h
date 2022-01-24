#pragma once


#include "view_interface.h"

template <class T>
class sstream_t : public gview_t {
 public:
     graph<T>* pgraph;
    
 public:
    virtual degree_t get_nebrs_out(vid_t vid, nebr_reader_t& header) {
        degree_t degree = get_degree_out(vid);
        header.ptr = pgraph->V[vid].getOutNeighbors();
        return degree;
    }

    virtual degree_t get_nebrs_in (vid_t vid, nebr_reader_t& header) {
        degree_t degree = get_degree_in(vid);
        header.ptr = pgraph->V[vid].getInNeighbors();
        return degree;
    }
    virtual degree_t get_degree_out(vid_t vid) {
        return pgraph->V[vid].getOutDegree(); 
    }
    virtual degree_t get_degree_in (vid_t vid) {
        return pgraph->V[vid].getInDegree(); 
    }
    
    virtual status_t    update_view() {
        status_t ret = update_view_help();
        if (ret == eEndBatch) return ret;
        return eOK;
    }
    virtual void        update_view_done() {assert(0);}
    void    init_view(graph<T>* G, ubatch_t* a_ubatch, vid_t a_vcount, index_t a_flag, index_t slide_sz1) {
        //set array members to 0;
        gview_t::init_view(a_ubatch, a_vcount, a_flag, slide_sz1);
        pgraph = G;
    }
    
   
    inline sstream_t() {
    } 
    inline virtual ~sstream_t() {
        //We may need to free some internal memory.XXX
    } 
};

template <class T>
sstream_t<T>* reg_sstream_view(graph<T>* G, ubatch_t* ubatch, vid_t v_count, typename callback<dst_id_t>::sfunc func,
                               index_t flag,  index_t slide_sz = 0, void* algo_meta = 0)
{
    sstream_t<T>* sstreamh = new sstream_t<T>;
    
    sstreamh->init_view(G, ubatch, v_count, flag, slide_sz);
    //sstreamh->sstream_func = func;
    sstreamh->algo_meta = algo_meta;
    
    if (IS_THREAD(flag)) {
        if (0 != pthread_create(&sstreamh->thread, 0, func, sstreamh)) {
            assert(0);
        }
        cout << "created sstream thread" << endl;
    }
    
    return sstreamh;
}
    
void unreg_view(gview_t* viewh)
{
    delete viewh;
}


