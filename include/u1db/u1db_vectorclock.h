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

#ifndef U1DB_VECTORCLOCK_H
#define U1DB_VECTORCLOCK_H

typedef struct _u1db_vectorclock_item {
    char *machine_id;
    int db_rev;
} u1db_vectorclock_item;

typedef struct _u1db_vectorclock {
    int num_items;
    u1db_vectorclock_item *items;
} u1db_vectorclock;

u1db_vectorclock *u1db__vectorclock_from_str(const char *s);

void u1db__free_vectorclock(u1db_vectorclock **clock);
int u1db__vectorclock_increment(u1db_vectorclock *clock,
                                const char *machine_id);

int u1db__vectorclock_maximize(u1db_vectorclock *clock,
                               u1db_vectorclock *other);
/**
 * Return a null-terminated string representation for this vector clock.
 * Callers must take care to free() the result.
 */
int u1db__vectorclock_as_str(u1db_vectorclock *clock, char **result);
int u1db__vectorclock_is_newer(u1db_vectorclock *maybe_newer,
                               u1db_vectorclock *older);


#endif // U1DB_VECTORCLOCK_H
