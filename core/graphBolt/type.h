#pragma once
#include <stdint.h>
#include <limits.h>
#include <string>
#include <time.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <map>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <atomic>
#include "list.h"
#include "bitmap.h"

using std::map;
using std::cout;
using std::endl;
using std::string;
using std::swap;
using std::pair;

typedef uint64_t index_t;

#ifdef B64
typedef uint16_t propid_t;
typedef uint64_t vid_t;
typedef uint64_t sid_t;
typedef uint64_t eid_t;
typedef uint32_t tid_t;
typedef uint64_t sflag_t;
typedef uint16_t qid_t;
typedef uint64_t snapid_t ;
//typedef uint16_t rdegree_t; //relative degree
typedef int32_t degree_t;
#elif B32
typedef uint8_t propid_t;
typedef uint32_t vid_t;
typedef uint32_t sid_t;
typedef uint32_t eid_t;
typedef uint8_t tid_t;
typedef uint64_t sflag_t;
typedef uint16_t qid_t;
typedef uint64_t snapid_t ;
//typedef uint16_t rdegree_t; //relative degree
typedef int32_t degree_t;
#endif

//typedef uint16_t vflag_t;

//Special v-unit flags
#define IS_HUB(flag) (flag & 0x1)
#define SET_HUB(flag) (flag | 0x1)
#define RESET_HUB(flag) (flag & ~0x1)

#define IS_CLEAN(flag) (flag & 0x2)
#define SET_CLEAN(flag) (flag | 0x2)
#define RESET_CLEAN(flag) (flag & ~0x2)
//#define VUNIT_NORMAL 0
//#define VUNIT_SHORT  1
//#define VUNIT_LONG   2
//#define TO_VUNIT_FLAG(flag)  (flag & 0x3)
//#define TO_VUNIT_COUNT(flag) ((flag >> 2 ) & 0x7)


//vsnasphot flags
#define IS_ARCHIVED(flag) (flag & 0x1)
#define SET_ARCHIVED(flag) (flag | 0x1)
#define RESET_ARCHIVED(flag) (flag & ~0x1)

#define IS_DURABLE(flag) (flag & 0x2)
#define SET_DURABLE(flag) (flag | 0x2)
#define RESET_DURABLE(flag) (flag & ~0x2)



extern degree_t PAGE;// = 4092;

#define HUB_COUNT  8192
#define MAX_DEL_DEGREE 500
#define MAX_DEGREE INT32_MAX

#ifndef PLAIN_GRAPH 
#ifdef B64
#define VBIT 40
#define VMASK 0xffffffffff
#define THIGH_MASK 0x7FFFFF0000000000
#define DEL_MASK   0x8000000000000000
#define SID_MASK   0x7FFFFFFFFFFFFFFF
#elif B32
#define VBIT 28
#define VMASK 0xfffffff
#define THIGH_MASK 0x70000000
#define DEL_MASK   0x80000000
#define SID_MASK   0x7FFFFFFF
#endif
#else
#ifdef B64
#define VBIT 40
#define VMASK 0xffffffffff
#define THIGH_MASK 0x7FFFFF0000000000
#define DEL_MASK   0x8000000000000000
#define SID_MASK   0x7FFFFFFFFFFFFFFF
#elif B32
#define VBIT 30
#define VMASK 0x3fffffff
#define THIGH_MASK 0x40000000
#define DEL_MASK   0x80000000
#define SID_MASK   0x7FFFFFFF
#endif
#endif

#define CL_MASK    0xFFFFFFFFFFFFFFC0
#define PAGE_MASK  0xFFFFFFFFFFFFFC00

#ifdef OVER_COMMIT
#define TO_CACHELINE(x) ((x+63) & CL_MASK)
#define TO_PAGESIZE(x) ((x+4095) & PAGE_MASK)
#else 
#define TO_CACHELINE(x) (x)
#define TO_PAGESIZE(x) (x)
#endif

#define TO_TID(sid)  ((sid & THIGH_MASK) >> VBIT)
#define TO_VID(sid)  (sid & VMASK)
#define TO_SID(sid)  (sid & SID_MASK)
#define TO_SUPER(tid) (((sid_t)(tid)) << VBIT)
#define TO_THIGH(sid) (sid & THIGH_MASK)
#define DEL_SID(sid) (sid | DEL_MASK)
#define IS_DEL(sid) (sid & DEL_MASK)
//#define UNDEL_SID(sid) (sid & SID_MASK)

#define TID_TO_SFLAG(tid) (1L << tid)
#define WORD_COUNT(count) ((count + 63) >> 6)

extern propid_t INVALID_PID;
extern tid_t    INVALID_TID;
extern sid_t    INVALID_SID;
extern degree_t INVALID_DEGREE;
//#define INVALID_PID 0xFFFF
//#define INVALID_TID 0xFFFFFFFF
//#define INVALID_SID 0xFFFFFFFFFFFFFFFF

#define NO_QID 0xFFFF

extern double  bu_factor;
extern int32_t MAX_BCOUNT; //256
extern uint64_t MAX_ECOUNT; //1000000
extern uint64_t MAX_PECOUNT;//670000

extern index_t  BATCH_SIZE;//
extern index_t  BATCH_MASK;//
extern index_t  BLOG_SHIFT;//
extern index_t  BUF_TX_SZ;
//extern index_t  BLOG_MASK;//
extern index_t  DELTA_SIZE;

extern index_t  W_SIZE;//Durable edge log offset
extern index_t  DVT_SIZE;
extern index_t  DURABLE_SIZE;//

extern index_t  OFF_COUNT;
extern int      THD_COUNT;
extern index_t  LOCAL_VUNIT_COUNT;
extern index_t  LOCAL_DELTA_SIZE;

//Concurrent View allowed.
#define VIEW_COUNT 8


//first two bits are for direction
#define TO_DIR_FLAG(flag) (flag & 0x3)

#define DOUBLE_EDGE_MASK (4L)
#define CREATE_EID_MASK (8L)
#define SNB_MASK (16L)
#define NO_VSNAP_THREAD (32L)
#define NO_SNAP_THREAD (64L)
#define SOURCE_BINARY (128L)
#define VERT_NOCREATE (256L)
#define NO_SNAP (512L)
#define NO_DURABLE (1024L)
#define CREATE_ETIME (2048L)

#define IS_DOUBLE_EDGE(flag) (flag & DOUBLE_EDGE_MASK)
#define IS_CREATE_EID(flag) (flag & CREATE_EID_MASK)
#define IS_SNB_FLAG(flag) (flag & SNB_MASK)
#define IS_NO_VSNAP_THREAD(flag) (flag & NO_VSNAP_THREAD)
#define IS_NO_SNAP_THREAD(flag) (flag & NO_SNAP_THREAD)
#define IS_SOURCE_BINARY(flag) (flag & SOURCE_BINARY)
#define IS_VERT_NOCREATE(flag) (flag & VERT_NOCREATE)
#define IS_NO_SNAP(flag) (flag & NO_SNAP)
#define IS_NO_DURABLE(flag) (flag & NO_DURABLE)
#define IS_CREATE_ETIME(flag) (flag & CREATE_ETIME)


void free_buf(void* buf);
void* alloc_buf();

off_t fsize(const string& fname);
off_t fsize(int fd);

enum direction_t {
    eout = 0, 
    ein
};

enum status_t {
    eOK = 0,
    eWIN,//A window is realized
    eTumble, //A window tumbles
    eEndBatch,//
    eInvalidPID,
    eInvalidVID,
    eQueryFail,
    eOOM,
    eDelete,
    eNoWork,
    eNotValid,
    eUnknown        
};

typedef union __univeral_type {
    //uint8_t  value_8b;
    //uint16_t value16b;
    tid_t    value_tid;
    //vid_t    value_vid;
    //sid_t    value_sid;
    //sid_t    value_offset;
    sid_t    value;

#ifdef B32   
    float    value_float;
    //char     value_string[4];
    //sid_t    value_charp;
#else     
    //char     value_string[8];
    //int64_t  value_64b;
    //eid_t    value_eid;
    //time_t   value_time;
    //char*    value_charp;
    double   value_float;
    double   value_double;
#endif
}univ_t;

typedef uint16_t word_t;
struct snb_t {
    word_t src;
    word_t dst;
};

union dst_id_t {
    sid_t sid;
    snb_t snb; 
};

//First can be nebr sid, while the second could be edge id/property
template <class T>
class dst_weight_t {
 public:
    dst_id_t first;
    T        second;
};

/*
class edge_t {
 public:
    sid_t src_id;
    dst_id_t dst_id;
    //void* weight;
    
    sid_t get_src() { return src_id;}
    void set_src(sid_t src) {src_id = src;}

    sid_t get_dst() { return dst_id.sid;}
    void set_dst(sid_t sid) { dst_id.sid = sid;}

    snb_t get_snb() { return dst_id.snb;}
    void set_snb(snb_t snb) { dst_id.snb = snb;}
    void  copy(edge_t* edge, int edge_size) {
        memcpy(edge, this, edge_size);
};*/

template <class T>
class  edgeT_t {
 public:
    sid_t src_id;
    T     dst_id;
    edgeT_t() {}
    sid_t get_src() { return src_id;}
    void set_src(sid_t src) {src_id = src;}
    sid_t get_dst();// { return ::get_dst(this);}
    void  set_dst(sid_t sid); // {::set_dst(this, sid);}
    void  set_snb(snb_t snb); // {::set_snb(this, sid);}
    void  copy(void* edge, int edge_size) {
        memcpy(edge, this, edge_size);
    }
    edgeT_t operator [] (int i) { assert(0); }
};


//Feel free to name the derived types, but not required.
typedef edgeT_t<dst_id_t> edge_t;

//deprecated
typedef dst_weight_t<univ_t> lite_edge_t;
typedef edgeT_t<lite_edge_t> ledge_t;

//These are new typedefs
typedef dst_weight_t<univ_t> weight_sid_t;
typedef edgeT_t<weight_sid_t> weight_edge_t;

#include "new_type.h"

// Functions on edgeT_t
inline sid_t get_dst(edge_t* edge) {
    return edge->dst_id.sid;
}
inline sid_t get_dst(edge_t& edge) {
    return edge.dst_id.sid;
}
inline void set_dst(edge_t* edge, sid_t dst_id) {
    edge->dst_id.sid = dst_id;
}
inline void set_dst(edge_t& edge, sid_t dst_id) {
    edge.dst_id.sid = dst_id;
}
inline void set_dst(edge_t* edge, snb_t dst_id) {
    edge->dst_id.snb = dst_id;
}

template <class T>
inline sid_t get_src(edgeT_t<T>* edge) {
    return edge->src_id;
}
template <class T>
inline sid_t get_src(edgeT_t<T>& edge) {
    return edge.src_id;
}
template <class T>
inline void set_src(edgeT_t<T>* edge, sid_t src_id) {
    edge->src_id = src_id;
}
template <class T>
inline void set_src(edgeT_t<T>& edge, sid_t src_id) {
    edge.src_id = src_id;
}

template <class T>
inline void set_snb(edgeT_t<T>* edge, snb_t snb) { 
    edge->dst_id.first.snb = snb;
}
inline void set_snb(edge_t* edge, snb_t snb) { 
    edge->dst_id.snb = snb;
}
template <class T>
inline sid_t get_dst(edgeT_t<T>* edge) { 
    return edge->dst_id.first.sid;
}
template <class T>
inline sid_t get_dst(edgeT_t<T>& edge) { 
    return edge.dst_id.first.sid;
}
template <class T>
inline snb_t get_snb(edgeT_t<T>* edge) { 
    return edge->dst_id.first.snb;
}
template <class T>
inline void set_dst(edgeT_t<T>* edge, sid_t dst_id) {
    edge->dst_id.first.sid = dst_id;
}
template <class T>
inline void set_dst(edgeT_t<T>& edge, sid_t dst_id) {
    edge.dst_id.first.sid = dst_id;
}

template <class T>
inline void set_weight_int(edgeT_t<T>* edge, sid_t weight) {
    assert(0);
    //edge->dst_id.second.value = weight;
}
template <class T>
inline void set_weight_int(edgeT_t<T>& edge, sid_t weight) {
    //edge.dst_id.second.value = weight;
    assert(0);
}

template <>
inline void set_weight_int<weight_sid_t>(edgeT_t<weight_sid_t>* edge, sid_t weight) {
    edge->dst_id.second.value = weight;
}
template <>
inline void set_weight_int<weight_sid_t>(edgeT_t<weight_sid_t>& edge, sid_t weight) {
    edge.dst_id.second.value = weight;
}

#ifdef B32
template <class T>
inline void set_weight_float(edgeT_t<T>* edge, float weight) {
    edge->dst_id.second.value_float = weight;
}
template <class T>
inline void set_weight_float(edgeT_t<T>& edge, float weight) {
    edge.dst_id.second.value_float = weight;
}
#elif B64
template <class T>
inline void set_weight_float(edgeT_t<T>* edge, double weight) {
    edge->dst_id.second.value_float = weight;
}
template <class T>
inline void set_weight_float(edgeT_t<T>& edge, double weight) {
    edge.dst_id.second.value_float = weight;
}
#endif

/*
template <class T>
inline int get_weight_int(edgeT_t<T>* edge) {
   return edge->dst_id.second.value;
}
*/
template <class T>
inline int get_weight_int(edgeT_t<T>& edge) {
    return edge.dst_id.second.value;
}
template <class T>
inline float get_weight_float(edgeT_t<T>& edge) {
    return edge.dst_id.second.value_float;
}
inline float get_weight_float(edge_t& edge) {
    return 0;
}

////function on dst_weight_t
template <class T> sid_t get_sid(T& dst);
template <class T> sid_t get_sid(T* dst);
template <class T> void set_sid(T& edge, sid_t sid1);

template <class T>
inline sid_t get_sid(T& dst)
{
    return dst.first.sid;
}
template <class T>
inline sid_t get_sid(T* dst)
{
    return dst->first.sid;
}

template <class T>
inline snb_t get_snb(T& dst)
{
    return dst.first.snb;
}
template <class T>
inline snb_t get_snb(T* dst)
{
    return dst->first.snb;
}

template <class T>
inline void set_sid(T& edge, sid_t sid1)
{
    edge.first.sid = sid1;
}
template <class T>
inline int get_weight_int(T& dst) {
    //assert(0);
    return 1;
}
/*
template <class T>
inline float get_weight_float(T& dst) {
    //assert(0);
    return 1.0;
}*/
template <>
inline int get_weight_int<dst_id_t>(dst_id_t& dst) {
   return 1;
}
template <>
inline int get_weight_int<weight_sid_t>(weight_sid_t& dst) {
    return dst.second.value;
}
template <class T>
inline int get_weight_int(T* dst) {
    //assert(0);
    return 1;
}
template <>
inline int get_weight_int<dst_id_t>(dst_id_t* dst) {
   return 1;
}
template <>
inline int get_weight_int<weight_sid_t>(weight_sid_t* dst) {
    return dst->second.value;
}
template <class T>
inline float get_weight_float(T* dst) {
    //assert(0);
    return 1.0;
}
template <>
inline float get_weight_float<dst_id_t>(dst_id_t* dst) {
   return 1;
}
template <>
inline float get_weight_float<weight_sid_t>(weight_sid_t* dst) {
    return dst->second.value_float;
}

//Specialized functions for plain graphs, no weights
template <>
inline void set_sid<dst_id_t>(dst_id_t& sid , sid_t sid1) {
    sid.sid = sid1;
}

template <class T>
inline void set_snb(T& dst, snb_t snb1)
{
    dst.first.snb = snb1;
}

template <>
inline void set_snb<dst_id_t>(dst_id_t& dst, snb_t snb1)
{
    dst.snb = snb1;
}

template<>
inline sid_t get_sid<dst_id_t>(dst_id_t& sid)
{
    return sid.sid;
}
template<>
inline sid_t get_sid<dst_id_t>(dst_id_t* sid)
{
    return sid->sid;
}

template<>
inline snb_t get_snb<dst_id_t>(dst_id_t& snb)
{
    return snb.snb;
}
template<>
inline snb_t get_snb<dst_id_t>(dst_id_t* snb)
{
    return snb->snb;
}

template <class T>
sid_t edgeT_t<T>::get_dst() { return ::get_dst(this);}

template <class T>
void  edgeT_t<T>::set_dst(sid_t sid) {::set_dst(this, sid);}

template <class T>
void  edgeT_t<T>::set_snb(snb_t snb) {::set_snb(this, snb);}

class vsnapshot_t {
 public:
    list_head list;
    index_t   id;
    int blog_id;
    int flag;
    
    //to be used for archiving and durability
    index_t tail;
    index_t marker;

    index_t gmarker;

    //local, total edge count
    index_t total_edges;

    //global, total edge count
    index_t total_gedges;

    inline void reset() {
        id = -1L;
        blog_id = -1;
        flag = 0;
        tail = -1L;
        marker = -1L;
    }
    inline void set_durable() {
        flag = 0;
    }
    inline bool get_durable() {
        return true;
    }
    inline void set_archived() {
        SET_ARCHIVED(flag);
    }
    inline bool get_archived() {
        return IS_ARCHIVED(flag);
    }
    inline vsnapshot_t* get_next() {
        return (vsnapshot_t*)list.get_next();
    }
    inline vsnapshot_t* get_prev() {
        return (vsnapshot_t*)list.get_prev();
    }
};

class snapshot_t {
 public:
    list_head list;
    snapid_t snap_id;
    vid_t cleaned_u;
    vid_t cleaned_d;
    std::atomic<int> ref_count;
    index_t marker;
    index_t gmarker;
    index_t* gmarkers;
    index_t* markers;
    int      count;

    snapshot_t(int a_count) {
        count = a_count;
        gmarkers = new index_t [count];
        markers = new index_t [count];
        reset();
    }

    void reset() {
        snap_id = 0;
        cleaned_u = 0;
        cleaned_d = 0;
        ref_count = 1;
        marker = 0;
        gmarker = 0;
        memset(gmarkers, 0, sizeof(index_t)*count);
        memset(markers, 0, sizeof(index_t)*count);
    }

    inline snapshot_t* take_ref() {
        ++ref_count;
        return this;
    }
    inline void drop_ref() {
        --ref_count;
        assert(ref_count > 0);
    }
    inline int get_ref() {
        return ref_count;
    }
    inline snapshot_t* get_next() {
        return (snapshot_t*)list.get_next();
    }
    inline snapshot_t* get_prev() {
        return (snapshot_t*)list.get_prev();
    }
    ~snapshot_t() {
        delete [] markers;
        delete [] gmarkers;
    }
};

class gsnapshot_t {
 public:
    list_head list;
    std::atomic<int> ref_count;
    index_t marker;
    index_t gmarker;

    gsnapshot_t() {
        ref_count = 1;
        marker = 0;
        gmarker = 0;
    }
    inline gsnapshot_t* take_ref() {
        ++ref_count;
        return this;
    }
    inline void drop_ref() {
        if (0 == --ref_count) {
            list_del(&list);
            delete this;
        }
    }
    inline int get_ref() {
        return ref_count;
    }
    inline gsnapshot_t* get_next() {
        return (gsnapshot_t*)list.get_next();
    }
    inline gsnapshot_t* get_prev() {
        return (gsnapshot_t*)list.get_prev();
    }
    ~gsnapshot_t() {
        
    }
};

class disk_snapshot_t {
 public:
    snapid_t snap_id;
    index_t  marker;
};


class pedge_t {
 public:
    propid_t pid;
    sid_t src_id;
    univ_t dst_id;
};

/*
template <class T>
class disk_kvT_t {
    public:
    vid_t    vid;
    T       dst;
};*/



//property name value pair
struct prop_pair_t {
    string name;
    string value;
};


template <class T>
class delentry_t {
 public:
    degree_t pos;
    T dst_id;
};


extern snapid_t get_snapid();
extern vid_t get_vcount(tid_t tid);

typedef struct __econf_t {
    string filename;
    string predicate;
    string src_type;
    string dst_type;
    string edge_prop;
} econf_t; 

typedef struct __vconf_t {
    string filename;
    string predicate;
} vconf_t; 

class pgraph_t;
class ubatch_t;

class gview_t;

template<class T>
class wstream_t;

template<class T>
class stream_t;

template<class T>
struct callback {
      //typedef void (*sfunc)(gview_t<T>*);
      typedef void* (*sfunc)(void*);

      typedef index_t (*parse_fn_t)(const string&, ubatch_t*, int64_t);
      typedef index_t (*parse_fn2_t)(const char*, ubatch_t*, index_t, int64_t);
      typedef index_t (*parse_fn3_t)(char*, edgeT_t<T>&);
};

