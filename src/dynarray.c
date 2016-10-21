#include "dynarray.h"
#include "zmalloc.h"

DynArray *create_dyn_array()
{
    return create_dyn_array_n(10);
}

DynArray *create_dyn_array_n(int size)
{
    DynArray *da = (DynArray *)zmalloc(sizeof(DynArray));
    da->cnt = 0;
    da->size = size > 0 ? size : 10;
    da->array = zcalloc(da->size * sizeof(void *));
    return da;
}

void destroy_dyn_array(DynArray *da)
{
    if (da)
    {
        if (da->array)
        {
            zfree(da->array);
            da->array = 0;
        }
        zfree(da);
    }
}

void destroy_dyn_array_ele(DynArray *da, FreeEleFunc f)
{
    if (da)
    {
        if (da->array)
        {
            int i;
            for (i = 0; i < da->cnt; i++)
            {
                f(da->array[i]);
            }

            zfree(da->array);
            da->array = 0;
        }
        zfree(da);
    }
}

int push_dyn_array(DynArray *da, void *e)
{
    if (!da || !e)
    {
        return 1;
    }
    if (da->cnt >= da->size)
    {
        da->size += 10;
        void **ar = (void **)zcalloc(da->size * sizeof(void *));
        int i;
        for (i = 0; i < da->cnt; i++)
        {
            ar[i] = da->array[i];
        }
        zfree(da->array);
        da->array = ar;
    }
    da->array[da->cnt++] = e;
    return 0;
}

