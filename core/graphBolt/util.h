#pragma once
#include <dirent.h>
#include <sys/stat.h>
#include <omp.h>
#include "type.h"
#include "new_func.h"
#include "wtime.h"

off_t fsize(const string& fname)
{
    struct stat st;
    if (0 == stat(fname.c_str(), &st)) {
        return st.st_size;
    }
    perror("stat issue");
    return -1L;
}

off_t fsize(int fd)
{
    struct stat st;
    if (0 == fstat(fd, &st)) {
        return st.st_size;
    }
    perror("stat issue");
    return -1L;
}

off_t fsize_dir(const string& idir)
{
    struct dirent *ptr;
    DIR *dir;
    string filename;
        
    index_t size = 0;
    index_t total_size = 0;
    

    //allocate accuately
    dir = opendir(idir.c_str());
    
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idir + "/" + string(ptr->d_name);
        size = fsize(filename);
        total_size += size;
    }
    closedir(dir);
    return total_size;
}

inline short CorePin(int coreID)
{
   int s, j;
   cpu_set_t cpuset;
   pthread_t thread;

   thread = pthread_self();

   /* Set affinity mask to include CPUs 0 to 7 */

   CPU_ZERO(&cpuset);
   CPU_SET(coreID, &cpuset);

   //for (j = 0; j < 8; j++) CPU_SET(j, &cpuset);

   s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0) {
       cout << "failed to set the core" << endl;
       //handle_error_en(s, "pthread_setaffinity_np");
   }

   /* Check the actual affinity mask assigned to the thread */
  /*
   s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0)
       handle_error_en(s, "pthread_getaffinity_np");

   printf("Set returned by pthread_getaffinity_np() contained:\n");
   for (j = 0; j < CPU_SETSIZE; j++)
       if (CPU_ISSET(j, &cpuset))
           printf("    CPU %d\n", j);
    */
   return 0;
}

inline index_t upper_power_of_two(index_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}

//You must be sure that it is a perfect power
inline int ilog2(index_t e)
{
    return __builtin_ctzll(e);
}

inline
index_t alloc_mem_dir(const string& idirname, char** buf, bool alloc)
{
    index_t total_size = fsize_dir(idirname);
    void* local_buf =  malloc(total_size);
    
    /*
    void* local_buf = mmap(0, total_size, PROT_READ|PROT_WRITE,
                MAP_PRIVATE|MAP_ANONYMOUS|MAP_HUGETLB|MAP_HUGE_2MB, 0, 0);

    if (MAP_FAILED == local_buf) {
        cout << "huge page alloc failed while reading input dir" << endl;
        local_buf =  malloc(total_size);
    }*/
    *buf = (char*)local_buf;
    return total_size;
}

inline 
index_t read_text_file(const string& filename, char* edges)
{
    FILE* file = fopen(filename.c_str(), "rb");
    assert(file != 0);
    index_t size = fsize(filename);
    if (size!= fread(edges, sizeof(char), size, file)) {
        assert(0);
    }
    return size;
}

//------- The APIs to use by higher level function -------//
inline 
index_t read_text_dir(const string& idirname, char*& edges)
{
    index_t dir_size = fsize_dir(idirname);
    edges = (char*)malloc(dir_size);
    
    //Read graph files
    struct dirent *ptr;
    FILE* file = 0;
    int file_count = 0;
    string filename;
    
    index_t size = 0;
    index_t total_size = 0;
    char* edge;
    
    double start = mywtime();
    DIR* dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idirname + "/" + string(ptr->d_name);
        file_count++;
        
        file = fopen((idirname + "/" + string(ptr->d_name)).c_str(), "rb");
        assert(file != 0);
        size = fsize(filename);
        edge = edges + total_size;
        if (size!= fread(edge, sizeof(char), size, file)) {
            assert(0);
        }
        total_size += size;
    }
    closedir(dir);
    double end = mywtime();
    cout << " Reading "  << file_count  << " file time = " << end - start << endl;
    //cout << "total size = " << total_size << endl;
    return total_size;
}

template <class T>
index_t read_idir_text(const string& idirname,  ubatch_t* ubatch,
                    typename callback<T>::parse_fn_t parse_and_insert, int64_t flags = 0)
{
    struct dirent *ptr;
    DIR *dir;
    int file_count = 0;
    string filename;
    string ofilename;

    //count the files
    dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        file_count++;
    }
    closedir(dir);
    assert(file_count !=0);
    
    string* ifiles = new string[file_count];
    int     icount = 0;
    
    //Read graph files:
    dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idirname + "/" + string(ptr->d_name);
        //cout << "ifile= "  << filename << endl ;
        ifiles[icount++] = filename;
    }
    closedir(dir);

    index_t edge_count = 0;
    double end ;
    int portion = icount/_numlogs;
    int my_start = _rank*portion;
    int my_end = my_start + portion;
    if (_rank == _numlogs - 1) my_end = icount;
    
    double start = mywtime();
    //cout << my_start << ":" << my_end << ":" << icount << endl;
    #pragma omp parallel num_threads(_num_sources) reduction(+:edge_count)
    {
        #pragma omp for schedule (static)
        for (int i = my_start; i < my_end; ++i) {
            edge_count += parse_and_insert(ifiles[i], ubatch, flags);
            //cout << edge_count << endl;
        }
    }
    end = mywtime();
    cout <<" Logging Time from Files = "<< end - start << endl;
    //cout << " vertex count = " << vid << endl;
    return edge_count;
}
/*
template <class T>
index_t read_idir_text2(const string& idirname,  ubatch_t* ubatch,
                    typename callback<T>::parse_fn2_t parse_and_insert, int64_t flags = 0)
{
    struct dirent *ptr;
    DIR *dir;
    int file_count = 0;
    string filename;
    string ofilename;

    //count the files
    dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        file_count++;
    }
    closedir(dir);
    assert(file_count !=0);
    
    string* ifiles = new string[file_count];
    int     icount = 0;
    
    //Read graph files
    dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idirname + "/" + string(ptr->d_name);
        //cout << "ifile= "  << filename << endl ;
        ifiles[icount++] = filename;
    }
    closedir(dir);
    
    index_t edge_count = 0;
    int portion = icount/_numlogs;
    int my_start = _rank*portion;
    int my_end = my_start + portion;
    if (_rank == _numlogs - 1) my_end = icount;
    //cout << my_start << ":" << my_end << ":" << icount << endl;
    index_t line = 0;
    #pragma omp parallel num_threads(_num_sources) reduction(+:line)
    {
        index_t total_size = 0;
        #pragma omp for schedule(static)
        for (int i = my_start; i < my_end; ++i) {
            total_size += fsize(ifiles[i]);
        }

        char* buf = (char*)malloc(total_size);
        index_t size = 0;
        #pragma omp for schedule (static)
        for (int i = my_start; i < my_end; ++i) {
            size += read_text_file(ifiles[i], buf + size);
        }
        //Now the batching starts
        double start = mywtime();
        line += parse_and_insert(buf, ubatch, total_size, flags);
        double end = mywtime();
        cout << "Logging time from in-memory data-source = " << end - start << endl;
    }
    return line;
}


template <class T>
index_t read_bin_dir(const string& idirname, edgeT_t<T>* edges)
{
    //Read graph files
    struct dirent *ptr;
    FILE* file = 0;
    int file_count = 0;
    string filename;
    
    index_t size = 0;
    index_t edge_count = 0;
    index_t total_edge_count = 0;
    edgeT_t<T>* edge;
    
    double start = mywtime();
    DIR* dir = opendir(idirname.c_str());
    while (NULL != (ptr = readdir(dir))) {
        if (ptr->d_name[0] == '.') continue;
        filename = idirname + "/" + string(ptr->d_name);
        file_count++;
        
        file = fopen((idirname + "/" + string(ptr->d_name)).c_str(), "rb");
        assert(file != 0);
        size = fsize(filename);
        edge_count = size/sizeof(edgeT_t<T>);
        edge = edges + total_edge_count;
        if (edge_count != fread(edge, sizeof(edgeT_t<T>), edge_count, file)) {
            assert(0);
        }
        total_edge_count += edge_count;
    }
    closedir(dir);
    double end = mywtime();
    cout << " Reading "  << file_count  << " file time = " << end - start << endl;
    //cout << "Total Edge Count = " << total_edge_count << endl;
    return total_edge_count;
}

*/
template <class T>
index_t read_idir(const string& idirname, edgeT_t<T>** pedges, bool alloc)
{
    //allocate accuately
    char* buf = 0;
    index_t total_size = alloc_mem_dir(idirname, &buf, alloc);
    index_t total_edge_count = total_size/sizeof(edgeT_t<T>);
    *pedges = (edgeT_t<T>*)buf;

    read_text_dir(idirname, (char*&)*pedges);
    
    /*if (total_edge_count != read_bin_dir(idirname, *pedges)) {
        assert(0);
    }*/
    
    
    return total_edge_count;
}

template <class T>
index_t add_edges_from_dir(const string& idirname, ubatch_t* ubatch, int64_t flags)
{
    CorePin(0);
    //Batch Graph
    double start = mywtime();
    index_t edge_count = 0;
    if (0 == IS_SOURCE_BINARY(flags)) {//text
        edge_count = read_idir_text<T>(idirname, ubatch, parsefile_and_insert<T>, flags);
    } else {//binary
        edge_count = read_idir_text<T>(idirname, ubatch, file_and_insert<T>, flags);
    }

    double end = mywtime();
    cout << "Batch Update Time (File) = " << end - start  
         << " Edge_count = " << edge_count << endl;  
    return edge_count;
}

