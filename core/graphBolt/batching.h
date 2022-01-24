#pragma once

#include <string>
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "type.h"
#include "list.h"
#include "log.h"

using std::string;


class ubatch_t {
public:    
    //circular edge log buffer
    blog_t* blog;
    int     blog_count;
        
    list_head  vsnapshot;
    vsnapshot_t* last_archived;
    vsnapshot_t* to_archived;
    vsnapshot_t* durable_vsnap;
    
    index_t total_edges;
    index_t total_gedges;
        
    //sizeof edgeT_t
    int     edge_size;
    int     wtf;   //edge log file
    string  wtfile;
    //reader for durability. Null if not enabled. 
    blog_reader_t* reader;

    //reader for archiving. Null if archiving is not performed, 
    //such as in logging nodes in distributed systems
    blog_reader_t* reader_archive;
    
public:
    ubatch_t(int a_edge_size, int num_sources) {
        INIT_LIST_HEAD(&vsnapshot);
        last_archived = 0;
        to_archived = 0;
        durable_vsnap = 0;
        edge_size = a_edge_size;
        blog_count = num_sources;
        blog = new blog_t[blog_count];
        for (int i = 0; i < blog_count; ++i) {
            blog[i].edge_size = edge_size;
        }
        reader = 0;
        reader_archive = 0;
        wtf = -1;
        total_edges = -1L;
        total_gedges = -1L;
    }
    inline void alloc_edgelog(index_t blog_shift) {
        for (int i = 0; i < blog_count; ++i) {
            blog[i].alloc_edgelog(blog_shift);
        }
    }
    inline blog_reader_t* register_reader(int& reg_id) {
        blog_reader_t* a_reader = new blog_reader_t[blog_count];
        for (int i = 0; i < blog_count; ++i) {
            a_reader[i].blog = blog + i;
            reg_id = blog[i].register_reader(a_reader + i);
        }
        return a_reader;
    }
    inline void unregister_reader(int reg_id) {
        for (int i = 0; i < blog_count; ++i) {
            blog[i].unregister_reader(reg_id);
        }
    }
    
    void reg_archiving() {
        int reg_id = 0;
        reader_archive = register_reader(reg_id);

        /*reader_archive = new blog_reader_t[blog_count];
        int reg_id = 0;
        for (int i = 0; i < blog_count; ++i) {
            reader_archive[i].blog = blog + i;
            reader_archive[i].tail = 0;
            reader_archive[i].marker = 0;
            reg_id = blog[i].register_reader(reader_archive + i);
        }*/
        return;
    }
    void reg_edgelog() {
        int reg_id = 0;
        reader = register_reader(reg_id);
        /*reader = new blog_reader_t[blog_count];
        for (int i = 0; i < blog_count; ++i) {
            reader[i].blog = blog + i;
            reader[i].tail = 0;
            reader[i].marker = 0;
            reg_id = blog[i].register_reader(reader + i);
        }*/
        return;
    }
    void waitfor_durable() {
        if (reader == 0) return;
        for (int i = 0; i < blog_count; ++i) {
            while (reader[i].tail < blog[i].blog_head) {
                usleep(100);
            }
        }
    }
    
    inline void set_total_edges () {
        index_t marker = 0;
        for (int i = 0; i < blog_count; ++i) {
            marker += blog[i].blog_head;
        }
        total_edges = marker; 
    }
    
    inline void set_total_edges (index_t a_ecount) {
        total_edges = a_ecount; 
    }
    inline index_t get_total_edges () {
        return total_edges;
    }

    inline void set_total_gedges (index_t a_ecount) {
        total_gedges = a_ecount; 
    }
    inline index_t get_total_gedges () {
        return total_gedges;
    }
    
    gsnapshot_t* create_gsnapshot(index_t gmarker, index_t edge_count, int i = 0) {
        return blog[i].create_gsnapshot(gmarker, edge_count);
    };
    inline gsnapshot_t* get_gsnapshot(int i = 0) {
        return blog[i].get_gsnapshot();
    }
    
    index_t update_marker() {
        if (0 == reader_archive) {
            free_blog();
            return eNoWork;
        }
        vsnapshot_t* startv = last_archived;
        vsnapshot_t* endv = to_archived;
        if (0 == endv || startv == endv) {
            for (int i = 0; i < blog_count; ++i) {
                reader_archive[i].tail = reader_archive[i].marker;
            }
            free_blog();
            return eNoWork;
        }
        int j = 0;
        do {
            if (startv) {
                startv = startv->get_prev();
            } else {
                startv = get_oldest_vsnapshot();
            }
            j  = startv->blog_id;
            reader_archive[j].marker = std::max(startv->marker, reader_archive[j].marker);
        } while (startv != endv);

        for (int i = 0; i < blog_count; ++i) {
            reader_archive[i].tail = reader_archive[i].marker;
        }

        free_blog();
        last_archived = to_archived;
        return 0;
    }

    //create micro-batching from the batched data.
    status_t  create_mbatch()
    {
        //create micro batch. It won't return without creating one as timeout is inf
        create_marker(0, -1L);//no timeout

        vsnapshot_t* startv = get_archived_vsnapshot();
        //vsnapshot_t* endv   = get_vsnapshot();
        
        //This code is required when we use a timeout
        /*if (startv == endv) {
            ubatch->update_marker();
            return eNoWork;
        }*/
        
        if (0 == startv) {
            startv = get_oldest_vsnapshot();
        } else {
            startv = startv->get_prev();
        }

        //so, we definitely have new data
        to_archived = startv;
        
        if (startv->total_edges == get_total_edges()) return eEndBatch;

        return eOK;
    }
    
    //should be called inside update_view() in case of sequential tests
    void free_blog() {
        for (int i = 0; i < blog_count; ++i) {
            blog[i].free_blog();
        }
    }
    
    inline vsnapshot_t* get_vsnapshot() {
        if (list_empty(&vsnapshot)) {
            return 0;
        } else { 
            return ((vsnapshot_t*) vsnapshot.get_next());
        }
    }
    inline vsnapshot_t* get_oldest_vsnapshot() {
        if (list_empty(&vsnapshot)) {
            return 0;
        } else { 
            return ((vsnapshot_t*) vsnapshot.get_prev());
        }
    }
    //the last vsnap upto which we already archived.
    inline vsnapshot_t* get_archived_vsnapshot() {
        return last_archived;
    }
    //the first nonarchived vsnap, from which we can archive now.
    //We expect this to be called from archive threads
    inline vsnapshot_t* get_from_vsnapshot() {
        vsnapshot_t* vsnap = last_archived;
        if (vsnap) {
            return vsnap->get_prev();//
        }else {
            return get_oldest_vsnapshot();
        }
    }
    inline vsnapshot_t* get_to_vsnapshot() {
        return to_archived;
    }

    //called from w thread 
    inline status_t write_edgelog() {
        vsnapshot_t* startv = durable_vsnap;
        vsnapshot_t* endv = get_vsnapshot();;
        if (0 == endv || startv == endv) {
            return eNoWork;
        }
        int j = 0;
        do {
            if (startv) {
                startv = startv->get_prev();
            } else {
                startv = get_oldest_vsnapshot();
            }
            j  = startv->blog_id;
            index_t w_tail = startv->tail;
            index_t w_marker = startv->marker; 
            index_t w_count = w_marker - w_tail;
            if (w_count == 0) continue;
            
            index_t actual_tail = w_tail & blog[j].blog_mask;
            index_t actual_marker = w_marker & blog[j].blog_mask;

            //cout << "writing " << w_tail << "\t:" << w_marker << "  ";
            //edge_t* tmp;
            
            char* buf = (char*)blog[j].blog_beg;
            if (actual_tail < actual_marker) {
                write(wtf, buf + actual_tail*edge_size, edge_size*w_count);
                //tmp  = (edge_t*)(buf + actual_tail*edge_size);
                //cout << tmp->src_id << "," << get_dst(tmp) << endl;
            }
            else {
                write(wtf, buf + actual_tail*edge_size, edge_size*(blog[j].blog_count - actual_tail));
                write(wtf, buf, edge_size*actual_marker);
                assert(blog[j].blog_count + actual_marker - actual_tail == w_count);
                //tmp  = (edge_t*)(buf + actual_tail*edge_size);
                //cout << tmp->src_id << "," << get_dst(tmp) << endl;
            }
            startv->set_durable();
            reader[j].tail = w_marker;
        } while (startv != endv);
        
        durable_vsnap = endv;

        //Write the string weights if any
        //this->mem.handle_write();

        if(endv->total_edges == get_total_edges()) return eEndBatch;
        
        //fsync();
        return eOK;
    }

    inline status_t create_vsnap() {
        vsnapshot_t* vsnap = 0;
        vsnapshot_t* old_vsnap = 0;
        gsnapshot_t* gsnap = 0;
        index_t gcount = 0;
        index_t ecount = 0;
        for (int i = 0; i < blog_count; ++i) {
            gsnap = get_gsnapshot(i);
            if (gsnap == 0) { //|| gsnap->marker == blog[i].blog_vmarker) 
                assert(0);
                continue;
            }
            gcount = gsnap->gmarker - blog[i].blog_vgmarker;
            ecount = gsnap->marker - blog[i].blog_vmarker;

            vsnap = (vsnapshot_t*)malloc(sizeof(vsnapshot_t));
            vsnap->blog_id = i;
            vsnap->tail = blog[i].blog_vmarker;
            vsnap->marker = gsnap->marker;
            vsnap->gmarker = gsnap->gmarker;
            blog[i].blog_vmarker = gsnap->marker;
            blog[i].blog_vgmarker = gsnap->gmarker;
            
            old_vsnap = get_vsnapshot();
            if (old_vsnap) {
                vsnap->id = old_vsnap->id + 1;
                vsnap->total_edges = old_vsnap->total_edges + ecount;
                vsnap->total_gedges = old_vsnap->total_gedges + gcount;
            } else {
                vsnap->id = 1; // ID starts at 1;
                vsnap->total_edges = ecount;
                vsnap->total_gedges = gcount;
            }

            list_add(&vsnap->list, &vsnapshot);
        }
        
        if (vsnap == 0) {
            return eNoWork;
        }
        
        //Let us do the durability here.
        if (0 != reader) write_edgelog();

        /*
        if (!this->reader_archive) {//the the batching node
           return create_snapshot();
        }*/
        if (get_total_edges() == vsnap->total_edges) return eEndBatch;
        return eOK;
    }
    inline status_t batch_edge(void* edge, int i /*=0*/) {
        status_t ret = eOK;
        index_t index = blog[i].batch_edge((edge_t*)edge);
        return ret; 
    }

    inline void reuse(const string& odir, const string& longname, const string& shortname)
    {
        file_close_open_edge();

        //deal with edge log buffers
        for (int i = 0; i < blog_count; ++i) {
            blog[i].reuse();
        }

        //delete vsnapshots
        while (!list_empty(&vsnapshot)) {
            list_del(vsnapshot.next);
        }

        last_archived = 0;
        to_archived = 0;
        durable_vsnap = 0;
        total_edges = -1L;
        total_gedges = -1L;
    }

    inline void file_open_edge(const string& a_file, bool trunc) {
        char name[16];
        sprintf(name, "%d", _rank);
        //filename = dir + col_info[0]->p_name; 
        string wtfile = a_file + ".elog" + name;
        if (trunc) {
            wtf = open(wtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
        } else {
            wtf = open(wtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
        }
        
        //string etfile = filename + ".str";
        //this->mem.file_open(etfile.c_str(), trunc);
        /*
        this->snapfile =  dir + this->col_info[0]->p_name + ".snap";
        if (trunc) {
            this->snap_f = fopen(this->snapfile.c_str(), "wb");//write + binary
        } else {
            this->snap_f = fopen(this->snapfile.c_str(), "r+b");
        }
        
        assert(this->snap_f != 0);
        */
    }

    void file_close_edge() {
        if (-1 != wtf) {
            close(wtf);
            wtf = -1;
        }
        //this->mem.file_close();
    }

    void file_close_open_edge() {
        if (-1 != wtf) {
            close(wtf);
            wtf = -1;
            file_open_edge(wtfile, true);
        }
        //this->mem.file_close();
    }
    void* get_prior_edges(index_t start_offset, index_t end_offset, void* edges) {
        assert(wtf != -1);
        index_t size = (end_offset - start_offset)*edge_size;
        index_t offset = start_offset*edge_size;
        //edgeT_t<T>* edges = (edgeT_t<T>*)malloc(size);
        assert(end_offset <= reader[0].tail);
        index_t sz_read = pread(wtf, edges, size, offset);
        
        assert(size == sz_read);
        //edge_t* tmp = (edge_t*)edges;
        //cout << "reding " << start_offset << ":" << end_offset <<"  "<< tmp->src_id << "," << get_dst(tmp) << endl;
        return edges;
    }
    inline status_t create_marker(index_t marker, index_t timeout /*= BATCH_TIMEOUT*/) {
        //batching nodes
        gsnapshot_t* gsnap = 0;
        //Backdoor code for some tests
        if (marker != 0) {
            assert(1 == blog_count);
            index_t snap_marker = marker;
            while (snap_marker > blog->blog_head) {
                usleep(1000); //sleep because batching was slow 
                if (snap_marker > get_total_edges()) {
                    snap_marker = get_total_edges();
                    break;
                }
            }

            index_t blog_marker = 0;
            gsnap = blog->get_gsnapshot();
            if (gsnap) {
                blog_marker = gsnap->marker;
            }
            blog->create_gsnapshot(snap_marker, snap_marker - blog_marker);
            create_vsnap();
            return eOK;
        } //Backdoor code ends.

        index_t blog_marker = 0;
        vsnapshot_t* vsnap = get_vsnapshot();
        if (vsnap) {
            blog_marker = vsnap->total_edges;
        }
        index_t new_marker = 0;
        index_t total_timeout = 0;
        while (total_timeout < timeout) {
            for (int i = 0; i < blog_count; ++i) {
                new_marker += blog[i].blog_head;
            }
            //how to force here as required in many case
            //above timeout condition will take care
            if (new_marker < blog_marker + BATCH_SIZE && 
                   new_marker != get_total_edges()) {
                update_marker();
                total_timeout += 1000; 
                usleep(1000); //One time sleep 
                new_marker = 0;
            } else {
                break;
            }
        }

        //create gsnapshot
        index_t snap_marker = 0;
        for (int i = 0; i < blog_count; ++i) {
            snap_marker = min(blog[i].blog_head, blog[i].blog_vmarker + BATCH_SIZE);

            if (snap_marker - blog[i].blog_free > blog[i].blog_count) {
                snap_marker = blog[i].blog_free + blog[i].blog_count;
            }
            blog_marker = 0;
            gsnap = blog[i].get_gsnapshot();
            if (gsnap) {
                blog_marker = gsnap->marker;
            }
            blog[i].create_gsnapshot(snap_marker, snap_marker - blog_marker);
        }

        //from gsnap create vsnapshot
        return create_vsnap();
    } 
};
