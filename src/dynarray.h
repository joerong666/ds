#ifndef _DYN_ARRAY_H_
#define _DYN_ARRAY_H_

typedef struct DynArray_t
{
    void **array;
    int cnt;
    int size;
} DynArray;

typedef void (*FreeEleFunc)(void *);

extern DynArray *create_dyn_array();
extern DynArray *create_dyn_array_n(int size);
extern void destroy_dyn_array(DynArray *da);
extern void destroy_dyn_array_ele(DynArray *da, FreeEleFunc f);
extern int push_dyn_array(DynArray *da, void *e);

#endif /* _DYN_ARRAY_H_ */

