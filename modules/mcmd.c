/*****************************************************************************\
 *  $Id$
 *****************************************************************************
 *  Copyright (C) 2001-2002 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Jim Garlick <garlick@llnl.gov>.
 *  UCRL-CODE-2003-005.
 *  
 *  This file is part of Pdsh, a parallel remote shell program.
 *  For details, see <http://www.llnl.gov/linux/pdsh/>.
 *  
 *  Pdsh is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *  
 *  Pdsh is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *  
 *  You should have received a copy of the GNU General Public License along
 *  with Pdsh; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.
\*****************************************************************************/

/*
 * Started with BSD mcmd.c which is:
 * 
 * Copyright (c) 1983, 1993, 1994, 2003
 *      The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *      This product includes software developed by the University of
 *      California, Berkeley and its contributors.
 *
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * 5. This is free software; you can redistribute it and/or modify it
 *    under the terms of the GNU General Public License as published
 *    by the Free Software Foundation; either version 2 of the
 *    License, or (at your option) any later version.
 *                              
 * 6. This is distributed in the hope that it will be useful, but
 *    WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *                                                           
 * 7. You should have received a copy of the GNU General Public License;
 *    if not, write to the Free Software Foundation, Inc., 59 Temple
 *    Place, Suite 330, Boston, MA  02111-1307  USA.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#if defined(LIBC_SCCS) && !defined(lint)
static char sccsid[] = "@(#)mcmd.c      Based from: 8.3 (Berkeley) 3/26/94";
#endif /* LIBC_SCCS and not lint */

#if     HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/param.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>

#ifdef HAVE_PTHREAD
#include <pthread.h>
#endif

#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#if HAVE_FCNTL_H
#include <fcntl.h>
#endif
#include <netdb.h>
#if HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <pwd.h>
#include <errno.h>
#include <ctype.h>
#include <string.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <munge.h>

#include "dsh.h"       /* LINEBUFSIZE && IP_ADDR_LEN */
#include "err.h"
#include "fd.h"
#include "mod.h"

#define MRSH_PORT       21212

#ifdef HAVE_PTHREAD
#define SET_PTHREAD()           pthread_sigmask(SIG_BLOCK, &blockme, &oldset)
#define RESTORE_PTHREAD()       pthread_sigmask(SIG_SETMASK, &oldset, NULL)
#define EXIT_PTHREAD()          RESTORE_PTHREAD(); \
                                return -1
#else
#define SET_PTHREAD()
#define RESTORE_PTHREAD()
#define EXIT_PTHREAD()          return -1
#endif

MODULE_TYPE(          "rcmd"                             );
MODULE_NAME(          "mrsh"                             );
MODULE_DESCRIPTION(   "mrsh rcmd connect method"         );
MODULE_AUTHOR(        "Mike Haskell <haskell5@llnl.gov>" );

struct pdsh_module_operations pdsh_module_ops = {
         NULL,
         NULL,
         NULL,
         NULL
};

int mcmd_init(opt_t *);
int mcmd_signal(int, int);
int mcmd(char *, char *, char *, char *, int *);

int
mcmd_init(opt_t * opt)
{
        /* not implemented */
        return 0;
}

int
mcmd_signal(int fd, int signum)
{
        char c;

        if (fd >= 0) {
                /* set non-blocking mode for write - just take our best shot */
                if (fcntl(fd, F_SETFL, O_NONBLOCK) < 0)
                        err("%p: fcntl: %m\n");
                c = (char) signum;
                write(fd, &c, 1);
        }
        return 0;
}

/*
 * Derived from the mcmd() libc call, with modified interface.
 * This version is MT-safe.  Errors are displayed in pdsh-compat format.
 * Connection can time out.
 *      ahost (IN)              target hostname
 *      addr (IN)               4 byte internet address
 *      remuser (IN)            remote username
 *      cmd (IN)                remote command to execute under shell
 *      fd2p (IN)               if non NULL, return stderr file descriptor here
 *      int (RETURN)            -1 on error, socket for I/O on success
 *
 * Originally by Mike Haskell for mrsh, modified slightly to work with pdsh by:
 * - making mcmd always thread safe
 * - using "err" function output errors.
 * - passing in address as addr intead of calling gethostbyname
 * - using default mshell port instead of calling getservbyname
 * 
 */
int 
mcmd(char *ahost, char *addr, char *remuser, char *cmd, int *fd2p)
{
        struct sockaddr m_socket;
        struct sockaddr_in *getp;
        struct sockaddr_in stderr_sock;
        struct sockaddr_in sin, from;
        struct sockaddr_storage ss;
        struct in_addr m_in;
        unsigned int randy, rand, randl;
        unsigned char *hptr;
        int s, lport, rv, rand_fd;
        int mcount;
        int s2, s3;
        char c;
        char num[6] = {0};
        char *mptr;
        char *mbuf;
        char *tmbuf;
        char haddrdot[16] = {0};
        char *m;
        char num_seq[12] = {0};
        size_t len;
        ssize_t m_rv;
        sigset_t blockme;
        sigset_t oldset;

        sigemptyset(&blockme);
        sigaddset(&blockme, SIGURG);
        sigaddset(&blockme, SIGPIPE);
        SET_PTHREAD();

        if (( rv = strcmp(ahost,"localhost")) == 0 ) {
                errno = EACCES;
                err("%p: %S: mcmd: Can't use localhost\n", ahost);
        }

        /*
         * Generate a random number to send in our package to the 
         * server.  We will see it again and compare it when the
         * server sets up the stderr socket and sends it to us.
         */
        rand_fd = open ("/dev/urandom", O_RDONLY | O_NONBLOCK);
        if ( rand_fd < 0 ) {
                err("%p: %S: mcmd: Open of /dev/urandom failed: %m\n", ahost);
                EXIT_PTHREAD();
        }

        m_rv = read (rand_fd, &randy, sizeof(uint32_t));
        if (m_rv < 0) {
                close(rand_fd);
                err("%p: %S: mcmd: Read of /dev/urandom failed: %m\n", ahost);
                EXIT_PTHREAD();
        }
        if (m_rv < (int) (sizeof(uint32_t))) {
                close(rand_fd);
                err("%p: %S: mcmd: Read of /dev/urandom returned too few bytes\n", ahost);
                EXIT_PTHREAD();
        }

        close(rand_fd);

        /*
         * Convert to decimal string...
         */
        snprintf(num_seq, sizeof(num_seq),"%d",randy);

        /*
         * Start setup of the stdin/stdout socket...
         */
        lport = 0;
        len = sizeof(struct sockaddr_in);

        s = socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0) {
                err("%p: %S: mcmd: socket call stdout failed: %m\n", ahost);
                EXIT_PTHREAD();
        }

        memset (&ss, '\0', sizeof(ss));
        ss.ss_family = AF_INET;

        rv = bind(s, (struct sockaddr *)&ss, len); 
        if (rv < 0) {
                err("%p: %S: mcmd: bind failed: %m\n", ahost);
                EXIT_PTHREAD();
        }

        sin.sin_family = AF_INET;

        memcpy(&sin.sin_addr.s_addr, addr, IP_ADDR_LEN); 

        sin.sin_port = htons(MRSH_PORT);
        rv = connect(s, (struct sockaddr *)&sin, sizeof(sin));
        if (rv < 0) {
                err("%p: %S: mcmd: connect failed: %m\n", ahost);
                EXIT_PTHREAD();
        }

        /*
         * Start the socket setup for the stderr.
         */
        lport = 0;
        if (fd2p == 0) {
                err("%p: %S: mcmd: no stderr defined\n", ahost);
                EXIT_PTHREAD();
        }

        s2 = socket(AF_INET, SOCK_STREAM, 0);
        if (s2 < 0) {
                close(s);
                err("%p: %S: mcmd: socket call for stderr failed: %m\n", ahost);
                EXIT_PTHREAD();
        }

        memset (&stderr_sock, 0, sizeof(stderr_sock));
        stderr_sock.sin_family = AF_INET;
        stderr_sock.sin_addr.s_addr = htonl(INADDR_ANY);
        stderr_sock.sin_port = 0;

        if (bind(s2, (struct sockaddr *)&stderr_sock, sizeof(stderr_sock)) < 0) {
                err("%p: %S: bind failed: %m\n", ahost);
                EXIT_PTHREAD();
        }
                
        len = sizeof(struct sockaddr);

        /*
         * Retrieve our port number so we can hand it to the server
         * for the return (stderr) connection...
         */

        /* getsockname is thread safe */
        if (getsockname(s2,&m_socket,&len) < 0) {
                close(s);
                err("%p: %S: getsockname failed: %m\n", ahost);
                EXIT_PTHREAD();
        }

        getp = (struct sockaddr_in *)&m_socket;
        lport = ntohs(getp->sin_port);

        snprintf(num,sizeof(num),"%d",lport);

        memcpy(&m_in.s_addr, addr, IP_ADDR_LEN);

        /* inet_ntoa is not thread safe, so we use the following,
         * which is more or less ripped from glibc
         */
        hptr = (unsigned char *)&m_in;
        sprintf(haddrdot, "%u.%u.%u.%u", hptr[0], hptr[1], hptr[2], hptr[3]);

        rv = listen(s2, 1);
        if (rv < 0) {
                close(s);
                close(s2);
                err("%p: %S: mcmd: listen() failed: %m\n", ahost);
                EXIT_PTHREAD();
        }

        /*
         * We call munge_encode which will take what we write in and return a
         * pointer to an munged buffer.  What we get back is a null terminated
         * string of encrypted characters.
         * 
         * The format of the unmunged buffer is as follows (each a string terminated 
         * with a '\0' (null):
         *                                              SIZE            EXAMPLE
         *                                              ==========      =============
         * remote_user_name                             variable        "mhaskell"
         * '\0'
         * dotted_decimal_address_of_this_server        7-15 bytes      "134.9.11.155"
         * '\0'
         * stderr_port_number                           4-8 bytes       "50111"
         * '\0'
         * /dev/urandom_client_produced_number          1-8 bytes       "1f79ca0e"
         * '\0'
         * users_command                                variable        "ls -al"
         * '\0' '\0'
         *
         * (The last extra null is accounted for in the following line's last strlen() call.)
         */


        mcount = ((strlen(remuser)+1) + (strlen(haddrdot)+1) + (strlen(num)+1) + 
                                           (strlen(num_seq)+1) + strlen(cmd)+2);
        tmbuf = mbuf = malloc(mcount);
        if (tmbuf == NULL) {
                close(s);
                close(s2);
                err("%p: %S: mcmd: Error from malloc\n", ahost);
                EXIT_PTHREAD();
        }
        /*
         * The following memset() call takes the extra trailing null as part of its
         * count as well.
         */
        memset(mbuf,0,mcount);

        mptr = strcpy(mbuf, remuser);
        mptr += strlen(remuser)+1;
        mptr = strcpy(mptr, haddrdot);
        mptr += strlen(haddrdot)+1;
        mptr = strcpy(mptr, num);
        mptr += strlen(num)+1;
        mptr = strcpy(mptr, num_seq);
        mptr += strlen(num_seq)+1;
        mptr = strcpy(mptr, cmd);

        if ((m_rv = munge_encode(&m,0,mbuf,mcount)) != EMUNGE_SUCCESS) {
                fprintf(stderr,"%s\n",munge_strerror((munge_err_t)m_rv));
                err("%p: %S: mcmd: munge_encode: %m\n", ahost);
        }

        mcount = (strlen(m)+1);
        
        /*
         * Write stderr port in the clear in case we can't decode for
         * some reason (i.e. bad credentials).
         */
        m_rv = fd_write_n(s, num, strlen(num)+1);
        if (m_rv != sizeof(num)) {
                 close(s);
                 close(s2);
                 free(m);
                 free(tmbuf);
                 if (m_rv == -1) {
                         if (errno == SIGPIPE) {
                                  err("%p: %S: mcmd: Lost connection (SIGPIPE).", ahost);
                                  EXIT_PTHREAD();
                         } else {
                                  err("%p: %S: mcmd: Write of stderr port num to socket failed: %m\n", ahost);
                                  EXIT_PTHREAD();
                         }
                 }
        }

        /*
         * Write the munge_encoded blob to the socket.
         */
        m_rv = fd_write_n(s, m, mcount);
        if (m_rv != mcount) {
                close(s);
                close(s2);
                free(m);
                free(tmbuf);
                if (m_rv == -1) {
                        if (errno == SIGPIPE) {
                                err("%p: %S: mcmd: Lost connection (SIGPIPE): %m\n", ahost);
                                EXIT_PTHREAD();
                        } else {
                                err("%p: %S: mcmd: Write to socket failed: %m\n", ahost);
                                EXIT_PTHREAD();
                        }
                }
        }

        free(m);
        free(tmbuf);

        errno = 0;
        len = sizeof(from); /* arg to accept */

        s3 = accept(s2, (struct sockaddr *)&from, &len);
        if (s3 < 0) {
                fprintf(stderr,"%s: ",ahost);
                lport = 0;
                close(s2);
                close(s);
                err("%p: %S: mcmd: accept (stderr) failed: %m\n", ahost);
                EXIT_PTHREAD();
        }

        close(s2);

        /*
         * Read from our stderr.  The server should have placed our random number
         * we generated onto this socket.
         */
        m_rv = fd_read_n(s3, &rand, sizeof(rand));
        if (m_rv != (ssize_t) (sizeof(rand))) {
                close(s);
                close(s3);
                err("%p: %S: mcmd: Bad read of expected verification "
                    "number off of stderr socket: %m\n", ahost);
                EXIT_PTHREAD();
        }

        randl = ntohl(rand);
        if (randl != randy) {
                char tmpbuf[LINEBUFSIZE] = {0};
                char *tptr = &tmpbuf[0];

                memcpy(tptr,(char *) &rand,sizeof(rand));
                tptr += sizeof(rand);
                m_rv = fd_read_line (s3, tptr, LINEBUFSIZE - sizeof(rand));
                if (m_rv < 0) {
                        if (lport)
                                close(*fd2p);
                        close(s);
                        err("%p: %S: mcmd: Bad read of error from stderr: %m\n", ahost);
                        EXIT_PTHREAD();
                }
                err("%p: %S: mcmd: Error: %s\n", ahost, &tmpbuf[0]);
                close(s);
                close(s3);
                EXIT_PTHREAD();
        }

        /*
         * Set the stderr file descriptor for the user...
         */
        *fd2p = s3;
        from.sin_port = ntohs((u_short)from.sin_port);
        if (from.sin_family != AF_INET) {
                fprintf(stderr,"%s: ",ahost);
                if (lport)
                        close(*fd2p);
                close(s);
                err("%p: %S: mcmd: socket: protocol failure in circuit setup\n", ahost);
                EXIT_PTHREAD();
        }

        m_rv = read(s, &c, 1);
        if (m_rv < 0) {
                if (lport)
                        close(*fd2p);
                close(s);
                err("%p: %S: mcmd: read: protocol failure: %m\n", ahost);
                EXIT_PTHREAD(); 
        }

        if (m_rv != 1) {
                if (lport)
                        close(*fd2p);
                close(s);
                err("%p: %S: mcmd: read: protocol failure: invalid response\n", ahost);
                EXIT_PTHREAD();
        }

        if (c != '\0') {
                /* retrieve error string from remote server */
                char tmpbuf[LINEBUFSIZE];
        
                m_rv = fd_read_line (s, &tmpbuf[0], LINEBUFSIZE);
                if (m_rv < 0) {
                        fprintf(stderr,"Error from %s: %s\n",ahost,&tmpbuf[0]);
                        if (lport)
                                close(*fd2p);
                        close(s);
                        err("%p: %S: mcmd: Error from remote host\n", ahost);
                        EXIT_PTHREAD();
                }
        }
        RESTORE_PTHREAD();

        return (s);
}

int
pdsh_rcmd(char *ahost, char *addr, char *luser, char *ruser, char *cmd,
          int rank, int *fd2p)
{   
        return mcmd(ahost, addr, ruser, cmd, fd2p);
}

int
pdsh_signal(int fd, int signum)
{   
        return mcmd_signal(fd, signum); 
}

int
pdsh_rcmd_init(opt_t * opt)
{   
        return mcmd_init(opt);
}
