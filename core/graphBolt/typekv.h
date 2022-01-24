#pragma once
//#include "onestr.h"
//#include "str2sid.h"

class tinfo_t {
 public:
    vid_t   max_vcount;
    vid_t   vert_id;
    tid_t   tid;
    char*   type_name;
    //strkv_t strkv;
};

//type class
class typekv_t  {
  private:
    
    //map <string, sid_t> str2vid;
	//str2intmap str2vid;// = new str2intmap();	
    
    //mapping between type-name(string) and type-id
    map<string, tid_t> str2enum;
    
    //for each type/class, the count of vertices  
    tinfo_t*    t_info;
    tid_t       t_count;

    tid_t       max_count;

    FILE*   vtf;   //vertex table file

  public:

    typekv_t() {
        init_enum(256);
        vtf = 0;
    }

    tid_t manual_setup(vid_t vert_count, bool create_vert, const string& type_name) {
        str2enum[type_name.c_str()] = t_count;
        t_info[t_count].type_name = strdup(type_name.c_str());

        if (create_vert) {
            t_info[t_count].vert_id = TO_SUPER(t_count) + vert_count;
        } else {
            t_info[t_count].vert_id = TO_SUPER(t_count);
        }
        t_info[t_count].max_vcount = vert_count;
        //t_info[t_count].strkv.setup(t_count, vert_count);
        return t_count++;//return the tid of this type
    }

    void init_enum(int enumcount) {
        max_count = enumcount;
        t_count = 0;
        t_info = new tinfo_t [enumcount];
        memset(t_info, 0, sizeof(tinfo_t)*enumcount);
    }
    
    inline  vid_t get_type_scount(tid_t type) {
        return TO_VID(t_info[type].max_vcount);
    }
    inline vid_t get_type_vcount(tid_t type) {
        return TO_VID(t_info[type].vert_id);
    }
    inline const char* get_type_name(tid_t type) {
        return t_info[type].type_name;
    }
    inline tid_t get_total_types() {
        return t_count;
    }

    inline sid_t get_sid(const char* src) {
        sid_t str2vid_iter = 0; //str2vid.find(src);
        return str2vid_iter;
    }

    /*
	inline sid_t get_sid(const char* src) {
        map<string, sid_t>::iterator str2vid_iter = str2vid.find(src);
        if (str2vid_iter == str2vid.end()) {
            return INVALID_SID;
        }
        return str2vid_iter->second;
    }
	*/

    inline string get_vertex_name(sid_t sid) {
        tid_t tid = TO_TID(sid);
        vid_t vid = TO_VID(sid);
        return "";
        //return t_info[tid].strkv.get_value(vid);
        //return t_info[tid].log_beg + t_info[tid].vid2name[vid];
    }
    
    sid_t add_vertex(const string& src, const string& dst);
    sid_t add_vertex(const string& src, tid_t type_id = 0) {
        assert(0);
        sid_t       src_id = 0;
        /*
        sid_t       super_id = 0;
        vid_t       vid = 0;

        assert(type_id < t_count);

        super_id = t_info[type_id].vert_id;

        //allocate class specific ids.
        sid_t str2vid_iter = str2vid.find(src);
        if (INVALID_SID == str2vid_iter) {
            src_id = super_id++;
            t_info[type_id].vert_id = super_id;
            str2vid.insert(src, src_id);

            vid     = TO_VID(src_id); 
            assert(vid < t_info[type_id].max_vcount);
            
            t_info[type_id].strkv.set_value(vid, src.c_str());
        } else {
            //dublicate entry 
            //If type mismatch, delete original //XXX
            src_id = str2vid_iter;
            tid_t old_tid = TO_TID(src_id);

            if (old_tid != type_id) {
                //Different types, delete
                assert(0);
                return INVALID_SID;
            }
        }*/
        return src_id;

    }
    
    void make_graph_baseline();
    virtual void store_graph_baseline(bool clean = false) {
        fseek(vtf, 0, SEEK_SET);

        //write down the type info, t_info
        char type_text[512];
        for (tid_t t = 0; t < t_count; ++t) {
#ifdef B32
            sprintf(type_text, "%u %u %s\n", t_info[t].max_vcount, TO_VID(t_info[t].vert_id), 
                    t_info[t].type_name);
#elif B64
            sprintf(type_text, "%lu %lu %s\n", t_info[t].max_vcount, TO_VID(t_info[t].vert_id), 
                    t_info[t].type_name);
#endif    
            fwrite(type_text, sizeof(char), strlen(type_text), vtf);
            //t_info[t].strkv.handle_write();
        }
        //str2enum: No need to write. We make it from disk during initial read.
        //XXX: write down the deleted id list
    }
    void read_graph_baseline();
    void file_open(const string& dir, bool trunc) {
        string filename = dir + ".type";
        string vtfile;
        
        vtfile = filename + ".vtable";

        if(trunc) {
            //vtf = open(vtfile.c_str(), O_RDWR|O_CREAT|O_TRUNC, S_IRWXU);
            vtf = fopen(vtfile.c_str(), "w");
            assert(vtf != 0); 
        } else {
            //vtf = open(vtfile.c_str(), O_RDWR|O_CREAT, S_IRWXU);
            vtf = fopen(vtfile.c_str(), "r+");
            assert(vtf != 0); 
        }
        for (tid_t t = 0; t < t_count; ++t) {
            //t_info[t].strkv.file_open(filename, trunc);
        }
    }
};
