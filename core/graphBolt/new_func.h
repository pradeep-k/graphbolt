#pragma once

#include "type.h"
#include "typekv.h"

extern typekv_t* typekv;

//-----Two high level functions can be used by many single stream graph ----
//----- The generic versions are for binary files/buffers.------
//----- Specialized versions are for text files/buffers.------

template <class T>
index_t parse_line(char* line, edgeT_t<T>& edge)
{
    cout << "No plugin found for parsing";
    assert(0);
    return 0;
}

//--------------- netflow function ------------------
// Actual parse function, one line at a time
template <>
inline index_t parse_line<netflow_dst_t>(char* line, edgeT_t<netflow_dst_t>& netflow) 
{
    if (line[0] == '%' || line[0]=='#') {
        return eNotValid;
    }
    
    //const char* del = ",\n";
    char* token = 0;
    
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.time = atoi(token);
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.duration = atoi(token);
    
    token = strtok_r(line, ",\n", &line);
    netflow.src_id = typekv->add_vertex(token);
    token = strtok_r(line, ",\n", &line);
    set_dst(netflow, typekv->add_vertex(token));
    
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.protocol = atoi(token);
    
    token = strtok_r(line, ",\n", &line);
    if (token[0] == 'P') {
        netflow.dst_id.second.src_port = atoi(token+4);
    } else {
        netflow.dst_id.second.src_port = atoi(token);
    }
    token = strtok_r(line, ",\n", &line);
    if (token[0] == 'P') {
        netflow.dst_id.second.dst_port = atoi(token+4);
    } else {
        netflow.dst_id.second.dst_port = atoi(token);
    }
    token = strtok_r(line, ",\n", &line);
    
    netflow.dst_id.second.src_packet = atoi(token);
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.dst_packet = atoi(token);
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.src_bytes = atoi(token);
    token = strtok_r(line, ",\n", &line);
    netflow.dst_id.second.dst_bytes = atoi(token);
    
    return eOK;
}

// Actual parse function, one line at a time
template <>
inline index_t parse_line<dst_id_t>(char* line, edgeT_t<dst_id_t>& edge)
{
    if (line[0] == '%' || line[0]=='#') {
        return eNotValid;
    }
    
    const char* del = " \t\n";
    char* token = 0;
    
    #ifdef B32
    int32_t sid; 
    token = strtok_r(line, del, &line);
    sscanf(token, "%u", &sid);
    token = strtok_r(line, del, &line);
    sscanf(token, "%u", &edge.dst_id.sid);
    #else
    int64_t sid; 
    token = strtok_r(line, del, &line);
    sscanf(token, "%lu", &sid);
    token = strtok_r(line, del, &line);
    sscanf(token, "%lu", &edge.dst_id.sid);
    #endif
    set_src(edge, sid);

    return eOK;
}

template <>
// Actual parse function, one line at a time
inline index_t parse_line<weight_sid_t>(char* line, edgeT_t<weight_sid_t>& edge)
{
    if (line[0] == '%' || line[0]=='#') {
        return eNotValid;
    }
    
    const char* del = " \t\n";
    char* token = 0;
    
    #ifdef B32
    int32_t sid; 
    float   weight;
    token = strtok_r(line, del, &line);
    sscanf(token, "%u", &sid);
    set_src(edge, sid);
    
    token = strtok_r(line, del, &line);
    sscanf(token, "%u", &sid);
    set_dst(edge, sid);
    
    token = strtok_r(line, del, &line);
    sscanf(token, "%f", &weight);
    set_weight_float(edge, weight);

    #else
    int64_t sid; 
    double  weight;
    token = strtok_r(line, del, &line);
    sscanf(token, "%lu", &sid);
    set_src(edge, sid);
    
    token = strtok_r(line, del, &line);
    sscanf(token, "%lu", &sid);
    set_dst(edge, sid);
    
    token = strtok_r(line, del, &line);
    sscanf(token, "%lf", &weight);
    set_weight_float(edge, weight);
    
    #endif

    return eOK;
}

template <class T>
index_t parsebuf_and_insert(const char* buf, ubatch_t* ubatch, index_t count, int64_t flags) 
{
    if (0 == buf) {
        return 0;
    }
    int i = omp_get_thread_num(); 
    edgeT_t<T> edge;
    index_t icount = 0;
    const char* start = 0;
    const char* end = 0;
    //const char* buf_backup_for_debug = buf; 
    char  sss[512];
    char* line = sss;
        
    start = buf;
    buf = strchr(start, '\n');
    while (buf) {
        end = buf;
        memcpy(sss, start, end - start); 
        sss[end-start] = '\0';
        line = sss;
        if (eOK == parse_line(line, edge)) {
            ubatch->batch_edge(&edge, i);
            icount++;
        }
        start = buf + 1;
        buf = strchr(start, '\n');
    }
    return icount;
}

template <class T>
inline index_t parsefile_and_insert(const string& textfile, ubatch_t* ubatch, int64_t flags) 
{
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    
    int i = omp_get_thread_num(); 
    edgeT_t<T> wedge;
    index_t icount = 0;
	char sss[512];
    char* line = sss;

    bool create_eid = IS_CREATE_EID(flags);
    bool double_edge = IS_DOUBLE_EDGE(flags);
    bool create_time = IS_CREATE_ETIME(flags);

    if (true == create_eid) {
        edgeT_t<dst_id_t> edge;
        if (false == double_edge) {
            while (fgets(sss, sizeof(sss), file)) {
                line = sss;
                
                if (eOK == parse_line(line, edge)) {
                    wedge.set_src(edge.get_src());
                    set_dst(wedge, get_dst(edge));
                    set_weight_int(wedge, icount);
                    ubatch->batch_edge(&wedge, i);
                    icount++;
                }
            }
        } else { //double the edges
            while (fgets(sss, sizeof(sss), file)) {
                line = sss;
                
                if (eOK == parse_line(line, edge)) {
                    wedge.set_src(edge.get_src());
                    set_dst(wedge, get_dst(edge));
                    set_weight_int(wedge, icount);
                    ubatch->batch_edge(&wedge, i);
                    icount++;
                    //reverse edge
                    wedge.set_src(get_dst(edge));
                    set_dst(wedge, get_src(edge));
                    set_weight_int(wedge, icount);
                    ubatch->batch_edge(&wedge, i);
                    icount++;
                }
            }
        }
    } else if (true == create_time) {
        edgeT_t<dst_id_t> edge;
        sid_t timestamp;
        index_t offset = (BATCH_SIZE << 2);
        index_t mask_res = 0;
        while (fgets(sss, sizeof(sss), file)) {
            line = sss;
            
            if (eOK == parse_line(line, edge)) {
                wedge.set_src(edge.get_src());
                set_dst(wedge, get_dst(edge));
                timestamp = icount;
                mask_res = icount & 0xF;
                if ((0 == mask_res || 1 == mask_res) && icount > offset) {
                    set_weight_int(wedge, timestamp - (mask_res+1)*BATCH_SIZE);
                } else {
                    set_weight_int(wedge, timestamp);
                }
                ubatch->batch_edge(&wedge, i);
                icount++;
            }
        }
    } else { // Don't create edge id or anything.
        if (false == double_edge) {
            while (fgets(sss, sizeof(sss), file)) {
                line = sss;
                
                if (eOK == parse_line(line, wedge)) {
                    ubatch->batch_edge(&wedge, i);
                    icount++;
                }
            }
        } else { //so double the edges
            sid_t src, dst;
            while (fgets(sss, sizeof(sss), file)) {
                line = sss;
                
                if (eOK == parse_line(line, wedge)) {
                    ubatch->batch_edge(&wedge, i);
                    icount++;
                    //reverse edge
                    src = get_src(wedge);
                    dst = get_dst(wedge);
                    wedge.set_src(src);
                    set_dst(wedge, dst);
                    ubatch->batch_edge(&wedge, i);
                    icount++;
                }
            }
        }
    }
    fclose(file);
    return icount;
}

//uses buffered read, one binary edge at a time, and then insert
template <class T>
index_t buf_and_insert(const char* buf , ubatch_t* ubatch, index_t count, int64_t flag) 
{
    index_t edge_count = count/sizeof(edgeT_t<T>);
    edgeT_t<T>* edges = (edgeT_t<T>*)buf;
    edgeT_t<T> edge;
    int j = omp_get_thread_num();
    for (index_t i = 0; i < edge_count; ++i) {
        ubatch->batch_edge(edges+i, j);
    }
    return edge_count;
}
/*
template <>
inline index_t buf_and_insert<weight_sid_t>(const char* buf, ubatch_t* ubatch, index_t count, int64_t flags) 
{
    int j = omp_get_thread_num();
    index_t edge_count = 0;
    sid_t sid;
    ssize_t size = 0;
    
    bool create_eid = IS_CREATE_EID(flags);
    bool double_edge = IS_DOUBLE_EDGE(flags);
    
    if (true == create_eid) {
        edgeT_t<dst_id_t>* edge;
        edgeT_t<weight_sid_t> wedge;
        index_t icount = 0;
        if (false == double_edge) {
            index_t edge_count = count/sizeof(edgeT_t<dst_id_t>);
            edgeT_t<dst_id_t>* edges = (edgeT_t<dst_id_t>*)buf;
            for (index_t i = 0; i < edge_count; ++i) {
                edge = edges + i;
                wedge.set_src(edge->get_src());
                wedge.set_dst(edge->get_dst());
                set_weight_int(wedge, icount); 
                ubatch->batch_edge(&wedge, j);
                icount++;
            }
        } else { 
            assert(0); //XXX
            //double the edge
        } 
    } else {
        index_t edge_count = count/sizeof(edgeT_t<weight_sid_t>);
        edgeT_t<weight_sid_t>* edges = (edgeT_t<weight_sid_t>*)buf;
        for (index_t i = 0; i < edge_count; ++i) {
            ubatch->batch_edge(edges+i, j);
        }
        return edge_count;
        //
    }
    return 0;
}
*/

template <class T>
inline index_t file_and_insert(const string& textfile, ubatch_t* ubatch, int64_t flags) 
{
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    int i = omp_get_thread_num();
    index_t icount = 0;
    sid_t sid;
    ssize_t size = 0;
    edgeT_t<T> edge;
    
    bool create_eid = IS_CREATE_EID(flags);
    bool double_edge = IS_DOUBLE_EDGE(flags);
    
    if (true == create_eid) {
        edgeT_t<dst_id_t> edge1;
        if (true == double_edge) {
            while((size = fread(&edge1, sizeof(edge1), 1, file)) > 0) {
                edge.set_src(edge1.get_src());
                set_dst(edge, edge1.get_dst());
                set_weight_int(edge, icount); 
                ubatch->batch_edge(&edge, i);
                icount++;
                //reverse edge
                edge.set_src(get_dst(edge1));
                set_dst(edge, get_src(edge1));
                set_weight_int(edge, icount);
                ubatch->batch_edge(&edge, i);
                icount++;
            }

        } else { // no double edges
            while((size = fread(&edge1, sizeof(edge1), 1, file)) > 0) {
                edge.set_src(edge1.get_src());
                set_dst(edge, edge1.get_dst());
                set_weight_int(edge, icount); 
                ubatch->batch_edge(&edge, i);
                icount++;
            }
        } 
    } else {
        if (false == double_edge) {
            while((size = fread(&edge, sizeof(edge), 1, file)) > 0) {
                ubatch->batch_edge(&edge, i);
                icount++;
            }
        } else {//double edges
            sid_t src, dst;
            while((size = fread(&edge, sizeof(edge), 1, file)) > 0) {
                ubatch->batch_edge(&edge, i);
                icount++;
                //reverse edge
                src = get_src(edge);
                dst = get_dst(edge);
                edge.set_src(src);
                set_dst(edge, dst);
                ubatch->batch_edge(&edge, i);
                icount++;
            }
        }
    }
    fclose(file);
    return icount;
}


/* Use this function to convert text file to binary file*/
template <class T>
index_t parsefile_to_bin(const string& textfile, const string& ofile, typename callback<T>::parse_fn3_t parse_line)
{
    cout << "size calc start " << textfile << endl;
	size_t file_size = fsize(textfile.c_str());
    size_t estimated_count = file_size/sizeof(edgeT_t<T>);
    cout << "estimated size " << file_size << endl;
    
    FILE* file = fopen(textfile.c_str(), "r");
    assert(file);
    
    //write the binary file
    FILE* wfile = fopen(ofile.c_str(), "wb");
    assert(wfile);
    
    int esize = (1<<20);
    edgeT_t<T>* edges = (edgeT_t<T>*)calloc(esize, sizeof(edgeT_t<T>));
    assert(edges);
    
    index_t icount = 0;
	char sss[512];
    char* line = sss;
    cout << "reading now" << endl;
    while (fgets(sss, sizeof(sss), file)) {
        line = sss;
        parse_line(line, edges[icount]);
        icount++;
        if (icount == esize) {
            fwrite(edges, sizeof(edgeT_t<T>), icount, wfile);
            //cout << icount << endl;
            icount = 0;
        }
    }

    if (icount != 0) {
        fwrite(edges, sizeof(edgeT_t<T>), icount, wfile);
        cout << icount << endl;
        icount = 0;
    }
    
    fclose(file);
    
    fclose(wfile);
    
    free(edges);
    return 0;
}

