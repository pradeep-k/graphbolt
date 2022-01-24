#pragma once

#include <atomic>
#include <algorithm>
#include "list.h"

using std::min;

extern int _rank;

class blog_t;

class blog_reader_t {
 public:
    blog_t* blog;
    //from tail to marker in blog_beg
    index_t tail;
    index_t marker;
    inline blog_reader_t() {
        blog = 0;
        tail = 0; //-1L;
        marker = 0; //-1L;
    }
};

//edge batching buffer
//One for each parallel shared-nothing data source
class blog_t {
 public:
    edge_t* blog_beg;
    int     edge_size;
    //In memory size
    index_t     blog_count;
    index_t     blog_shift;
    //MASK
    index_t     blog_mask;

    //current batching position
    index_t     blog_head;
    //Make adj list from this point
    //Make adj list upto this point
    index_t     blog_vmarker;
    index_t     blog_vgmarker;
    //Due to rewind, head should not go beyond
    std::atomic<index_t>   blog_free;
    
    list_head  gsnapshot;

    //The readers must register here.
    blog_reader_t* reader[VIEW_COUNT];

    blog_t() {//XXX don't use this constructor
        blog_beg = 0;
        edge_size = 0; 
        blog_count = 0;
        blog_head = 0;
        blog_vmarker = 0;
        blog_vgmarker = 0;
        blog_free = 0;
        INIT_LIST_HEAD(&gsnapshot);
        
        memset(reader, 0, VIEW_COUNT*sizeof(blog_reader_t*));
    }
    
    blog_t(int a_edge_size) {
        blog_beg = 0;
        edge_size = a_edge_size; 
        blog_count = 0;
        blog_head = 0;
        blog_vmarker = 0;
        blog_vgmarker = 0;
        blog_free = 0;
        INIT_LIST_HEAD(&gsnapshot);
        
        memset(reader, 0, VIEW_COUNT*sizeof(blog_reader_t*));
    }

    inline void reuse() {
        blog_head = 0;
        blog_vmarker = 0;
        blog_vgmarker = 0;
        blog_free = 0;

        //delete gsnapshots
        while (!list_empty(&gsnapshot)) {
            list_del(gsnapshot.next);
        }

        //re-initialize the readers
        for (int reg_id = 0; reg_id < VIEW_COUNT; ++reg_id) { 
            if (reader[reg_id] != 0) {
                reader[reg_id]->tail = -1L;
                reader[reg_id]->marker = -1L;
            }
        }
    }

    gsnapshot_t* get_next(gsnapshot_t* snap) {
        gsnapshot_t* snap1 = snap->get_next();
        if (snap1 == (gsnapshot_t*)&gsnapshot) {
            return 0;
        }
        return snap1;
    }

    inline int register_reader(blog_reader_t* a_reader) {
        int reg_id = 0;
        for (; reg_id < VIEW_COUNT; ++reg_id) { 
            if (reader[reg_id] == 0) {
                reader[reg_id] = a_reader;
                return reg_id;
            }
        }
        assert(0);
        return reg_id;
    }
    inline void unregister_reader(int reg_id) {
        reader[reg_id] = 0;
    }

    inline index_t batch_edge(edge_t* edge) {
        index_t index = __sync_fetch_and_add(&blog_head, 1L);
        bool rewind = !((index >> blog_shift) & 0x1);
        sid_t sid;
        //index_t round = 0;

        while (index + 1 - blog_free > blog_count) {
            //cout << "Sleeping for edge log" << endl;
            //assert(0);
            usleep(100);
            /*++round;
            if (round > 100000) {
                cout << _rank << "Y" << endl;
                round = 0;
            }*/ 
        }
        sid = edge->get_src();
        bool is_del = IS_DEL(sid);
        #ifndef DEL
        if (is_del) assert(0);
        #endif
        if (is_del) { //this is just a negative no, not our format
            edge->set_src(DEL_SID(-sid));
        }
        
        index_t index1 = (index & blog_mask);
        edge_t* new_edge = (edge_t*)((char*)blog_beg + edge_size*index1);
        edge->copy(new_edge, edge_size);
        sid = edge->get_dst();
        if (rewind) {
            new_edge->set_dst(DEL_SID(sid));
        } else {
            new_edge->set_dst(TO_SID(sid));
        }
        return index;
    }

    inline void readfrom_snapshot(snapshot_t* global_snapshot) {
        blog_head = global_snapshot->marker;
        //blog_tail = global_snapshot->marker;
        //blog_marker = global_snapshot->marker;
        //blog_wtail = global_snapshot->durable_marker;
        //blog_wmarker = global_snapshot->durable_marker; 
    }

    inline void free_blog() {
        index_t min_marker = blog_vmarker;
        for (int reg_id = 0; reg_id < VIEW_COUNT; ++reg_id) {
            if (reader[reg_id] == 0) continue;
            min_marker = min(min_marker, reader[reg_id]->tail);
        }
        blog_free = min_marker;
    }

    void alloc_edgelog(index_t bit_shift) {
        if (blog_beg) {
            free(blog_beg);
            blog_beg = 0;
        }
        blog_shift = bit_shift;
        blog_count = (1L << blog_shift);
        blog_mask = blog_count - 1;
        //blog->blog_beg = (edgeT_t<T>*)mmap(0, sizeof(edgeT_t<T>)*blog->blog_count, 
        //PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);
        //if (MAP_FAILED == blog->blog_beg) {
        //    cout << "Huge page alloc failed for edge log" << endl;
        //}
        
        /*
        if (posix_memalign((void**)&blog_beg, 2097152, blog_count*sizeof(edgeT_t<T>))) {
            perror("posix memalign batch edge log");
        }*/
        blog_beg = (edge_t*)calloc(blog_count, edge_size);
        assert(blog_beg);
    }

    gsnapshot_t* create_gsnapshot(index_t gmarker, index_t edge_count)
    {
        gsnapshot_t* next = new gsnapshot_t;
        gsnapshot_t* last = 0;
        next->marker = edge_count;
        next->gmarker = gmarker;
        
        if (!list_empty(&gsnapshot)) {
            last = (gsnapshot_t*)gsnapshot.get_next();
            next->marker += last->marker;
        }
        
        list_add(&next->list, &gsnapshot);
        return next;
    }
    inline gsnapshot_t* get_gsnapshot() {
        if (list_empty(&gsnapshot)) {
            return 0;
        } else { 
            return ((gsnapshot_t*) gsnapshot.get_next())->take_ref();
        }
    }
};

