/*
 * Copyright 2011 Canonical Ltd.
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
#include "u1db/u1db_internal.h"


static void uuid_to_hex(char *hex_out, unsigned char *bin_in);

#if defined(_WIN32) || defined(WIN32)
#include "Wincrypt.h"

static HCRYPTPROV cryptProvider = 0;

static HCRYPTPROV getProvider()
{
    if (cryptProvider == 0) {
        if (!CryptAcquireContext(&cryptProvider, NULL, NULL, PROV_RSA_AES,
                                 CRYPT_VERIFYCONTEXT))
        {
            return 0;
        }
    }
    return cryptProvider;
}

int
u1db__generate_hex_uuid(char *uuid)
{
    HCRYPTPROV provider;
    unsigned char buf[16];

    provider = getProvider();
    if (provider == 0) {
        // TODO: This is really system failure, but we'll go with Invalid
        //       Parameter for now.
        return U1DB_INVALID_PARAMETER;
    }
    if (!CryptGenRandom(provider, 16, (char*)buf)) {
        // TODO: Probably want a better error here.
        return U1DB_NOMEM;
    }
    uuid_to_hex(uuid, buf);
    return U1DB_OK;
}

#else

#include "uuid/uuid.h"

int
u1db__generate_hex_uuid(char *uuid)
{
    uuid_t local_uuid;
    uuid_generate_random(local_uuid);
    uuid_to_hex(uuid, local_uuid);
    return U1DB_OK;
}

#endif // defined(_WIN32) || defined(WIN32)


static void
uuid_to_hex(char *hex_out, unsigned char *bin_in)
{
    int i;
    for (i = 0; i < 16; ++i) {
        hex_out[i*2] = (bin_in[i] >> 4);
        hex_out[i*2+1] = (bin_in[i] & 0x0F);
    }
    for (i = 0; i < 32; ++i) {
        if (hex_out[i] < 10) {
            hex_out[i] += '0';
        } else {
            hex_out[i] += 'a' - 10;
        }
    }
}

