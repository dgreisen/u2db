/*
 * Copyright 2012 Canonical Ltd.
 *
 * This file is part of u1db.
 *
 * u1db is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 3
 * as published by the Free Software Foundation.
 *
 * u1db is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with u1db.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <string.h>
#include <stdlib.h>
#include "u1db/u1db.h"
#include "u1db/compat.h"
#include "u1db/u1db_vectorclock.h"


#ifdef _MSC_VER
// Windows doesn't have strndup, so we fake one
static char *
_win32_strndup(const char *s, size_t n)
{
    char *out;
    out = (char*)calloc(1, n+1);
    if (out == NULL) {
        return NULL;
    }
    memcpy(out, s, n);
    out[n] = '\0';
    return out;
}
#endif //_MSC_VER

struct inserts_needed {
    struct inserts_needed *next;
    int other_offset;
    int clock_offset;
};

static void
free_inserts(struct inserts_needed **chain)
{
    struct inserts_needed *cur, *next;
    if (chain == NULL || *chain == NULL) {
        return;
    }
    cur = *chain;
    while (cur != NULL) {
        next = cur->next;
        free(cur);
        cur = next;
    }
    *chain = NULL;
}

void
u1db__free_vectorclock(u1db_vectorclock **clock)
{
    int i;
    char *replica_uid;
    if (clock == NULL || *clock == NULL) {
        return;
    }
    if ((*clock)->items != NULL) {
        for (i = 0; i < (*clock)->num_items; i++) {
            replica_uid = (*clock)->items[i].replica_uid;
            if (replica_uid != NULL) {
                free(replica_uid);
            }
        }
    }
    free((*clock)->items);
    free(*clock);
    *clock = NULL;
}

u1db_vectorclock *
u1db__vectorclock_from_str(const char *s)
{
    u1db_vectorclock *res = NULL;
    int i;
    const char *cur, *colon, *pipe, *end;
    const char *last_replica_uid;
    char *last_digit;
    if (s == NULL) {
        s = "";
    }
    end = s + strlen(s);
    res = (u1db_vectorclock *)calloc(1, sizeof(u1db_vectorclock));
    if (res == NULL) {
        return NULL;
    }
    if ((end - s) == 0) {
        // Empty string, no items
        res->items = NULL;
        res->num_items = 0;
        return res;
    }
    // Count the number of '|' symbols, and allocate buffers for it
    res->num_items = 1;
    for (cur = s; cur < end; cur++) {
        if (*cur == '|') {
            res->num_items += 1;
        }
    }
    res->items = (u1db_vectorclock_item*)calloc(res->num_items,
                                        sizeof(u1db_vectorclock_item));
    // Now walk through it again, looking for the machine:count pairs
    cur = s;
    last_replica_uid = "";
    for (i = 0; i < res->num_items; i++) {
        if (cur >= end) {
            // Ran off the end. Most likely indicates a trailing | that isn't
            // followed by content.
            u1db__free_vectorclock(&res);
            return NULL;
        }
        pipe = memchr(cur, '|', end-cur);
        if (pipe == NULL) {
            // We assume the rest of the string is what we want
            pipe = end;
        }
        colon = memchr(cur, ':', pipe-cur);
        if (colon == NULL || (colon - cur) == 0 || (pipe - colon) == 1) {
            // Either, no colon, no replica_uid, or no digits
            u1db__free_vectorclock(&res);
            return NULL;
        }
        res->items[i].replica_uid = strndup(cur, colon-cur);
        if (strcmp(res->items[i].replica_uid, last_replica_uid) <= 0) {
            // TODO: We don't have a way to indicate what the error is, but
            //       we'll just return NULL;
            // This entry wasn't in sorted order, so treat it as an error.
            u1db__free_vectorclock(&res);
            return NULL;
        }
        last_replica_uid = res->items[i].replica_uid;
        res->items[i].db_rev = strtol(colon+1, &last_digit, 10);
        if (last_digit != pipe) {
            u1db__free_vectorclock(&res);
            return NULL;
        }
        cur = pipe + 1;
    }
    return res;
}

int
u1db__vectorclock_increment(u1db_vectorclock *clock, const char *replica_uid)
{
    int i, cmp;
    u1db_vectorclock_item *new_buf;
    if (clock == NULL || replica_uid == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    for (i = 0; i < clock->num_items; ++i) {
        cmp = strcmp(replica_uid, clock->items[i].replica_uid);
        if (cmp == 0) {
            // We found the entry
            clock->items[i].db_rev++;
            return U1DB_OK;
        } else if (cmp < 0) {
            // replica_uid would come right before items[i] if it was present.
            // So we break, and insert it here
            break;
        }
    }
    // If we got here, then 'i' points at the location where we want to insert
    // a new entry.
    new_buf = (u1db_vectorclock_item*)realloc(clock->items,
        sizeof(u1db_vectorclock_item) * (clock->num_items + 1));
    if (new_buf == NULL) {
        return U1DB_NOMEM;
    }
    clock->items = new_buf;
    clock->num_items++;
    memmove(&clock->items[i + 1], &clock->items[i],
            sizeof(u1db_vectorclock_item) * (clock->num_items - i - 1));
    clock->items[i].replica_uid = strdup(replica_uid);
    clock->items[i].db_rev = 1;
    return U1DB_OK;
}

int
u1db__vectorclock_maximize(u1db_vectorclock *clock, u1db_vectorclock *other)
{
    int ci, oi, cmp;
    int num_inserts, move_to_end, num_to_move, item_size;
    struct inserts_needed *needed = NULL, *next = NULL;
    u1db_vectorclock_item *new_buf;

    if (clock == NULL || other == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    num_inserts = ci = oi = 0;
    // First pass, walk both lists, determining what items need to be inserted
    while (oi < other->num_items) {
        if (ci >= clock->num_items) {
            // We have already walked all of clock, so everything in other
            // gets appended
            next = (struct inserts_needed *)calloc(1, sizeof(struct inserts_needed));
            next->next = needed;
            needed = next;
            // We need the final offset, after everything has been moved.
            next->clock_offset = ci + num_inserts;
            next->other_offset = oi;
            num_inserts++;
            oi++;
            continue;
        }
        cmp = strcmp(clock->items[ci].replica_uid,
                     other->items[oi].replica_uid);
        if (cmp == 0) {
            // These machines are the same, take the 'max' value:
            if (clock->items[ci].db_rev < other->items[oi].db_rev) {
                clock->items[ci].db_rev = other->items[oi].db_rev;
            }
            ci++;
            oi++;
            continue;
        } else if (cmp < 0) {
            // clock[ci] comes before other[oi], so step clock
            ci++;
        } else {
            // oi comes before ci, so it needs to be inserted
            next = (struct inserts_needed *)calloc(1, sizeof(struct inserts_needed));
            next->next = needed;
            needed = next;
            next->clock_offset = ci + num_inserts;
            next->other_offset = oi;
            num_inserts++;
            oi++;
        }
    }
    if (num_inserts == 0) {
        // Nothing more to do
        return U1DB_OK;
    }
    // Now we need to expand the clock array, and start shuffling the data
    // around
    item_size = sizeof(u1db_vectorclock_item);
    new_buf = (u1db_vectorclock_item *)realloc(clock->items,
                item_size * (clock->num_items + num_inserts));
    if (new_buf == NULL) {
        free_inserts(&needed);
        return U1DB_NOMEM;
    }
    clock->items = new_buf;
    clock->num_items += num_inserts;
    next = needed;
    move_to_end = clock->num_items - 1;
    // Imagine we have 3 inserts, into an initial list 5-wide.
    // a c e g h, inserting b f i
    // Final length is 8,
    // i should have ci=7, num_inserts = 3
    // f should have ci=4, num_inserts = 2
    // b should have ci=1, num_inserts = 1
    // First step, we want to move 0 items, and just insert i at the end (7)
    // Second step, we want to move g & h from 3 4, to be at 5 6, and then
    // insert f into 4
    // Third step, we move c & e from 1 2 to 2 3 and insert b at 1
    while (next != NULL) {
        num_to_move = move_to_end - next->clock_offset;
        if (num_to_move > 0) {
            memmove(&clock->items[next->clock_offset + 1],
                    &clock->items[next->clock_offset - num_inserts + 1],
                    item_size * num_to_move);
        }
        clock->items[next->clock_offset].replica_uid = strdup(
            other->items[next->other_offset].replica_uid);
        clock->items[next->clock_offset].db_rev =
            other->items[next->other_offset].db_rev;
        num_inserts--;
        move_to_end = next->clock_offset - 1;
        next = next->next;
    }
    free_inserts(&needed);
    return U1DB_OK;
}

int
u1db__vectorclock_as_str(u1db_vectorclock *clock, char **result)
{
    int buf_size, i, val, count;
    char *cur, *fmt;
    // Quick pass, to determine the buffer size:
    buf_size = 0;
    if (result == NULL) {
        return U1DB_INVALID_PARAMETER;
    }
    if (clock == NULL) {
        // Allocate space for the empty string
        cur = (char *)calloc(1, 1);
        *result = cur;
        return U1DB_OK;
    }
    for (i = 0; i < clock->num_items; i++) {
        buf_size += strlen(clock->items[i].replica_uid);
        buf_size += 2; // ':' and possible '|'
        val = clock->items[i].db_rev;
        do {
            // divide by 8 is close to divide by 10, to get the number of
            // binary digits we will need to represent the decimal form
            val >>= 3;
            buf_size++;
        } while (val > 0);
    }
    cur = (char *)calloc(1, buf_size);
    *result = cur;
    for (i = 0; i < clock->num_items; i++) {
        if (i == 0) {
            fmt = "%s:%d";
        } else {
            fmt = "|%s:%d";
        }
        count = snprintf(cur, buf_size, fmt, clock->items[i].replica_uid,
                         clock->items[i].db_rev);
        cur += count;
        buf_size -= count;
    }
    return U1DB_OK;
}

int
u1db__vectorclock_is_newer(u1db_vectorclock *maybe_newer,
                           u1db_vectorclock *other)
{
    int ci, oi, cmp, is_newer, n_db_rev, o_db_rev;
    if (maybe_newer == NULL || maybe_newer->num_items == 0) {
        // NULL is never newer
        return 0;
    }
    if (other == NULL || other->num_items == 0) {
        // This is not NULL, so it should be newer, we may need to check if
        // self is the empty string, though.
        return 1;
    }
    ci = oi = 0;
    is_newer = 0;
    // First pass, walk both lists, determining what items need to be inserted
    while (oi < other->num_items && ci < maybe_newer->num_items) {
        cmp = strcmp(maybe_newer->items[ci].replica_uid,
                     other->items[oi].replica_uid);
        if (cmp == 0) {
            // Both clocks have the same machine, see if one is newer
            n_db_rev = maybe_newer->items[ci].db_rev;
            o_db_rev = other->items[ci].db_rev;
            if (n_db_rev < o_db_rev) {
                // At least one entry in other is newer than this
                return 0;
            } else if (n_db_rev > o_db_rev) {
                // If we have no conflicts, this is strictly newer
                is_newer = 1;
            }
            ci++;
            oi++;
            continue;
        } else if (cmp < 0) {
            // maybe_newer has an entry that other doesn't have, which would
            // make it newer
            is_newer = 1;
            ci++;
        } else {
            // other has an entry that maybe_newer doesn't have, so we must
            // not be strictly newer
            return 0;
        }
    }
    if (oi == other->num_items && ci < maybe_newer->num_items) {
        // ci has an entry that other doesn't have, it is newer
        is_newer = 1;
    }
    if (oi < other->num_items) {
        // We didn't walk all of other, which means it has an entry which ci
        // doesn't have, and thus maybe_newer is not strictly newer
        return 0;
    }
    return is_newer;
}
