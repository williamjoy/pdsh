/*****************************************************************************\
 *  $Id$
 *****************************************************************************
 *  Copyright (C) 2001-2006 The Regents of the University of California.
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
 *  This module uses the "pipecmd" interface to execute 
 *   arbitrary processes. First of the "remote" args is the
 *   command to execute. Some simple parameters are substituted 
 *   on the command line:
 * 
 *    %h :  Target "hostname"
 *    %u :  Remote username
 *    %n :  Remote "rank" (0-n)
 *
 *  E.g.:
 *   
 *   pdsh -Rexec -w foo[0-1] ssh -l %u -x %h hostname
 *
 *  would somewhat mimic the existing ssh module.
 *
 */

#if     HAVE_CONFIG_H
#include "config.h"
#endif


#include <sys/wait.h>
#include <string.h>
#include <unistd.h>

#include "src/pdsh/opt.h"
#include "src/pdsh/mod.h"
#include "src/pdsh/rcmd.h"
#include "src/common/pipecmd.h"
#include "src/common/err.h"
#include "src/common/xmalloc.h"

#if STATIC_MODULES
#  define pdsh_module_info kexeccmd_module_info
#  define pdsh_module_priority kexeccmd_module_priority
#endif    

int pdsh_module_priority = DEFAULT_MODULE_PRIORITY;

static int mod_kexec_postop(opt_t *opt);
static int mod_kexec_exit (void);

static int kexec_init(opt_t *);
static int kexec_signal(int, void *arg, int);
static int kexeccmd(char *, char *, char *, char *, char *, int, int *, void **); 
static int kexec_destroy (pipecmd_t p);

/*
 *  Export generic pdsh module operations:
 */
struct pdsh_module_operations kexeccmd_module_ops = {
    (ModInitF)       NULL,
    (ModExitF)       mod_kexec_exit,
    (ModReadWcollF)  NULL,
    (ModPostOpF)     mod_kexec_postop
};

/*
 *  Export rcmd module operations
 */
struct pdsh_rcmd_operations kexeccmd_rcmd_ops = {
    (RcmdInitF)    kexec_init,
    (RcmdSigF)     kexec_signal,
    (RcmdF)        kexeccmd,
    (RcmdDestroyF) kexec_destroy
};

/* 
 * Export module options
 */
struct pdsh_module_option kexeccmd_module_options[] = 
 { 
   PDSH_OPT_TABLE_END
 };

/* 
 * Sshcmd module info 
 */
struct pdsh_module pdsh_module_info = {
  "rcmd",
  "kexec",
  "Mark Grondona <mgrondona@llnl.gov>",
  "arbitrary command rcmd connect method",
  DSH,
  &kexeccmd_module_ops,
  &kexeccmd_rcmd_ops,
  &kexeccmd_module_options[0],
};


static int mod_kexec_postop(opt_t *opt)
{
    if (strcmp(opt->rcmd_name, "kexec") == 0) {
        if (opt->connect_timeout != CONNECT_TIMEOUT) {
            err("%p: Cannot specify -t with \"-R kexec\"\n");
            return 1;
        }
    }
    return 0;
}
static int kexec_init(opt_t * opt)
{
    /*
     * Drop privileges if running setuid root
     */
    if ((geteuid() == 0) && (getuid() != 0)) {
        if (setuid (getuid ()) < 0)
            errx ("%p: setuid: %m\n");
    }

    /*
     *  Do not resolve hostnames in pdsh when using exec
     */
    if (rcmd_opt_set (RCMD_OPT_RESOLVE_HOSTS, 0) < 0)
        errx ("%p: kexeccmd_init: rcmd_opt_set: %m\n");

    return 0;
}

static int mod_kexec_exit (void)
{
    return 0;
}

static int kexec_signal(int fd, void *arg, int signum)
{
    return (pipecmd_signal ((pipecmd_t) arg, signum));
}

static int
kexeccmd(char *ahost, char *addr, char *luser, char *ruser, char *cmd,
       int rank, int *fd2p, void **arg)
{
    pipecmd_t p;
    const char **argv = pdsh_remote_argv ();
    /*
     *  If pdsh_remote_argv is empty or NULL we may be running
     *   in interactive dsh mode. Don't try to split the cmd
     *   into args ourselves in this case, instead just pass
     *   to a shell:
     */
    const int KEXEC_PREFIX = 5;
    const char *alt_argv[] = { "echo", "kubectl", "exec", ahost, "--", "sh", "-c", cmd, NULL };
    const char **new_argv;
    int b_free_argv = 0;
    if (!argv || *argv == NULL)
        argv = alt_argv;
    else {
        int argv_len = 0;
        for(const char **s = argv; *s; s++, argv_len++);

        //TODO: free memory
        new_argv = (const char **) Malloc ( (KEXEC_PREFIX + argv_len ) * sizeof (char *));
        b_free_argv = 1;
        memset (new_argv, 0, KEXEC_PREFIX + argv_len+1);
        

        for(int i = 0; i < KEXEC_PREFIX; i++) {
            new_argv[i] = Strdup (alt_argv[i]);
        }
        for(int i =0; i <= argv_len; i++) {
            new_argv[KEXEC_PREFIX+i] = Strdup (argv[i]);
        }
        argv = new_argv;
    }

    if (!(p = pipecmd (argv[0], argv + 1,  ahost, ruser, rank)))
        return (-1);

    if (fd2p)
        *fd2p = pipecmd_stderrfd (p);

    *arg = p;

    int _fd = (pipecmd_stdoutfd (p));
    if (b_free_argv)
        Free(new_argv);
    return _fd;
}

static int 
kexec_destroy (pipecmd_t p)
{
    int status;

    if (pipecmd_wait (p, &status) < 0)
        return (1);

    pipecmd_destroy (p);

    return (WEXITSTATUS (status));
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
