#include <stdio.h>
#include "heap.h"
#include "assert.h"

int cmp(char *a, char *b) {
    int *__a = (int *) a;
    int *__b = (int *) b;
    return *__a - *__b;
}

int main(int argc, char **argv) {
    int myints[] = {10, 20, 30, 5, 15};
    Vector *v = NewVector(int, 5);
    for (int i = 0; i < 5; i++) {
        Vector_Push(v, myints[i]);
    }

    make_heap(v, 0, v->top, cmp);

    int n;
    Vector_Get(v, 0, &n);
    assert(30 == n);

    pop_heap(v, 0, v->top, cmp);
    v->top = 4;
    Vector_Get(v, 0, &n);
    assert(20 == n);

    Vector_Push(v, 99);
    push_heap(v, 0, v->top, cmp);
    Vector_Get(v, 0, &n);
    assert(99 == n);

    Vector_Free(v);
    printf("PASS!");
    return 0;
}

