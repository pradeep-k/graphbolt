#pragma once

#include "log.h"


#define STALE_MASK 0x1
#define PRIVATE_MASK 0x2
#define SIMPLE_MASK  0x4
#define V_CENTRIC 0x8
#define E_CENTRIC  0x10
#define C_THREAD  0x20
#define NO_DEGREE  0x40

#define SET_STALE(x) (x = (x | STALE_MASK))
#define SET_PRIVATE(x) (x = (x | PRIVATE_MASK))
#define SET_SIMPLE(x) (x = (x | SIMPLE_MASK))
#define SET_THREAD(x) (x = (x | C_THREAD))
#define SET_NO_DEGREE(x) (x = (x | NO_DEGREE))

#define IS_STALE(x) (x & STALE_MASK)
#define IS_PRIVATE(x) (x & PRIVATE_MASK)
#define IS_SIMPLE(x) (x & SIMPLE_MASK)
#define IS_THREAD(x) (x & C_THREAD)
#define IS_NO_DEGREE(x) (x & NO_DEGREE)

#define SET_V_CENTRIC(x) (x = (x | V_CENTRIC))
#define SET_E_CENTRIC(x) (x = (x | E_CENTRIC))
#define IS_V_CENTRIC(x) (x & V_CENTRIC)
#define IS_E_CENTRIC(x) (x & E_CENTRIC)

enum enumView {
    eStale = 1,
    ePrivate = 2,
    eSimple = 4,
    vCentric = 8,
    eCentric = 16,
    cThread = 32,
    eNoDegree = 64,
};

struct nebr_reader_t {
    void* ptr;
    degree_t prior_sz;
    degree_t degree;
    int T_size;
    
    nebr_reader_t() {
        prior_sz = 0;
        //ptr = malloc(prior_sz);
        T_size = sizeof(vid_t);
    }
    degree_t adjust_size(degree_t nebr_count, int a_T_size) {
        T_size = a_T_size;
        degree = nebr_count;
        if (nebr_count*T_size > prior_sz) {
            prior_sz = nebr_count*T_size;
            ptr = realloc(ptr, prior_sz);
        }
        return nebr_count;
    }
    template <class T>
    T* get_adjlist() {
        return (T*)ptr;
    }
    template <class T>
    T* get_item(degree_t i) {
        T* dst = (T*) ((char*)ptr + i*T_size);
        return dst;
        
    }
    sid_t get_sid(degree_t i) {
        dst_id_t* dst = (dst_id_t*) ((char*)ptr + i*T_size);
        return ::get_sid(*dst);
    }
    degree_t get_degree() {
        return degree;
    }

    ~nebr_reader_t() {
        if (prior_sz != 0) {
            free(ptr);
            ptr = 0;
            prior_sz = 0;
        }
    }
};


class gview_t {
 public:
    vsnapshot_t*    vsnapshot;
    vsnapshot_t*    prev_vsnapshot;
    ubatch_t*       ubatch;
    blog_reader_t* reader;
    edge_t*         new_edges;
    int             new_edge_count;
    int             blog_count;
    int             reg_id;
    
    index_t         update_marker;
    void*           algo_meta;//algorithm specific data
    vid_t           v_count;
    vid_t           changed_vout;
    vid_t           changed_vin;
    int             flag;
    int             update_count;
    Bitmap*         bitmap_out;      
    Bitmap*         bitmap_in;      
    index_t         slide_sz;
    index_t         ith_window_sz;
    pthread_t       thread;
 public: 
    
    virtual degree_t get_nebrs_out(vid_t vid, nebr_reader_t& header) {assert(0); return 0;}
    virtual degree_t get_nebrs_in (vid_t vid, nebr_reader_t& header) {assert(0); return 0;}
    virtual degree_t get_degree_out(vid_t vid) {assert(0); return 0;}
    virtual degree_t get_degree_in (vid_t vid) {assert(0); return 0;}
    
    virtual status_t    update_view() {assert(0); return eOK;}
    virtual void        update_view_done() {assert(0);}
    
	void    init_view(ubatch_t* a_ubatch, vid_t a_vcount, index_t a_flag, index_t slide_sz1) {
        ubatch = a_ubatch;
        v_count = a_vcount;
        bitmap_out = new Bitmap(v_count);
        if (is_ddir()) {
            bitmap_in = new Bitmap(v_count);
        } else {
            bitmap_in = bitmap_out;
        }

        if (slide_sz1 != 0) {
            slide_sz = slide_sz1/BATCH_SIZE;
            ith_window_sz = slide_sz;
        } else {
            slide_sz = slide_sz1;
            ith_window_sz = slide_sz;
        }

        if (reader == 0) {
            reader = ubatch->register_reader(reg_id);
        }
    }
    
    inline vid_t  get_vcount() { return v_count; }
    inline void   set_algometa(void* a_meta) {algo_meta = a_meta;}
    inline void*  get_algometa() {return algo_meta;}
    virtual index_t get_snapmarker() { return update_marker; }
    bool is_ddir() { return (1 == _dir);}
  
    status_t update_view_help() {
        status_t ret = eOK;
        //exit condition
        if (get_snapmarker() == ubatch->total_edges) return eEndBatch; 
        //Get the correct vsnapshot
        vsnapshot_t* new_vsnapshot = this->get_new_vsnapshot();
        while (new_vsnapshot == this->vsnapshot || new_vsnapshot->id < ith_window_sz) {
            index_t edge_count = ubatch->get_total_edges();
            if ((0 != new_vsnapshot) && (new_vsnapshot->total_edges == edge_count)) {//we have to exit now
                break;
            }
            usleep(1000);
            new_vsnapshot = this->get_new_vsnapshot();
        }
        //We need to slide so that each instance is realized.
        if (ith_window_sz > 0) {
            if (new_vsnapshot->id > ith_window_sz) {//as id starts with 1 
                //find older vsnapshot
                //vsnapshot_t* temp = new_vsnapshot;
                do {
                    new_vsnapshot = new_vsnapshot->get_next(); //older one 
                } while (new_vsnapshot->id != ith_window_sz);
                
                //increment ith_window_sz
                ith_window_sz += slide_sz;
                ret = eWIN;
            } else if (new_vsnapshot->id == ith_window_sz) {
                ith_window_sz += slide_sz;
                ret = eWIN;
            }
        }
		
        this->vsnapshot = new_vsnapshot;
        update_marker = new_vsnapshot->total_edges;

        //copy the new edges
		copy_new_edges();
        ++update_count;
        return eOK;
    }
	void copy_new_edges() {
		edge_t* buf;
		size_t edge_size = sizeof(edge_t);
        new_edge_count = vsnapshot->total_edges;

		if (prev_vsnapshot) {
			new_edge_count -= prev_vsnapshot->total_edges;
		}   
		new_edges = (edge_t*)realloc(new_edges, new_edge_count*edge_size);

		index_t gstart = 0;
		index_t gend = 0;
		index_t start = 0;
		index_t end = 0;
		index_t offset = 0;
		blog_t* blog = ubatch->blog;
		index_t total = blog->blog_count;


		int i = 0;
		do { //prev_vsnapshot gets updated to vsnapshot as well
			if (prev_vsnapshot) {
				prev_vsnapshot = prev_vsnapshot->get_prev();
			} else {
				prev_vsnapshot = ubatch->get_oldest_vsnapshot();
			}   

			i = prev_vsnapshot->blog_id;
			gstart = prev_vsnapshot->tail;
			gend = prev_vsnapshot->marker;
			if (gstart == gend) continue;

			start = gstart & blog[i].blog_mask;
			end = gend & blog[i].blog_mask;
			total = blog[i].blog_count;
			buf = (edge_t*)blog[i].blog_beg;
			//cout << start << " E " << end << endl;


			if (end > start) {
				memcpy(new_edges+offset, buf + start, (end-start)*edge_size);
				offset += end - start;
			} else {
				memcpy(new_edges+offset, buf + start, (total-start)*edge_size);
				offset += total - start;
				if (end != 0) {
					memcpy(new_edges+offset, buf, end*edge_size);
					offset += end;
				}
			}
			//copied, so don't block logging
			reader[i].tail = prev_vsnapshot->marker;
			reader[i].marker = prev_vsnapshot->marker;
		} while (prev_vsnapshot != vsnapshot);
		assert(new_edge_count == offset);



	}
    inline index_t get_new_edges(edge_t*& a_new_edges) {
        a_new_edges = new_edges;
        return new_edge_count;
    }
    
    inline vsnapshot_t* get_new_vsnapshot(bool update_reader = false) {
        vsnapshot_t* new_vsnapshot;
        new_vsnapshot = ubatch->get_archived_vsnapshot();
        return new_vsnapshot;
    }

    inline gview_t() {
        vsnapshot = 0;
        prev_vsnapshot = 0;
        update_marker = 0;
        algo_meta = 0;
        v_count = 0;
        flag = 0;
        reader = 0;
        reg_id = -1;
        update_count = 0;
        changed_vout = 0;
        changed_vin = 0;
        new_edges = 0;
        new_edge_count = 0;
    } 
    inline virtual ~gview_t() {
        //We may need to free some internal memory.XXX
    } 
};

//reading utility
inline void read_edge(blog_t* blog, index_t i, void* edge)
{
    //index_t round = 0;
    index_t e = (i & blog->blog_mask);
    bool rewind1 = !((i >> blog->blog_shift) & 0x1);
    edge_t* new_edge = (edge_t*)((char*)(blog->blog_beg) + blog->edge_size*e);
    volatile bool rewind2 = IS_DEL(new_edge->get_dst());
    while (rewind1 != rewind2) {
        /* ++round;
        if (round > 100000) {
            cout << _rank << "X" << round << endl;
            round == 0;
        }*/
        usleep(10);
        rewind2 = IS_DEL(new_edge->get_dst());
    }
    new_edge->copy(edge, blog->edge_size);
    edge_t* edge1 = (edge_t*) edge;
    edge1->set_dst(TO_SID(edge1->get_dst()));
}
