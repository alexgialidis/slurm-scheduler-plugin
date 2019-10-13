/*****************************************************************************\
 *  thesis_wrapper.c - plugin for Slurm's internal scheduler.
\*****************************************************************************/

#include <stdio.h>

#include "slurm/slurm_errno.h"

#include "src/common/plugin.h"
#include "src/common/log.h"
#include "src/common/node_select.h"
#include "src/common/slurm_priority.h"
#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/reservation.h"
#include "src/slurmctld/slurmctld.h"
#include "src/plugins/sched/thesis/thesis.h"

const char		plugin_name[]	= "Alex's Thesis Scheduler plugin";
const char		plugin_type[]	= "sched/thesis";
const uint32_t	plugin_version	= SLURM_VERSION_NUMBER;

static pthread_t thesis_thread = 0;
static pthread_mutex_t thread_flag_mutex = PTHREAD_MUTEX_INITIALIZER;

int init(void)
{
	sched_verbose("Thesis scheduler plugin loaded");

	slurm_mutex_lock( &thread_flag_mutex );
	if ( thesis_thread ) {
		debug2( "Thesis scheduler thread already running, "
			"not starting another" );
		slurm_mutex_unlock( &thread_flag_mutex );
		return SLURM_ERROR;
	}

	slurm_thread_create(&thesis_thread, thesis_agent, NULL);

	slurm_mutex_unlock( &thread_flag_mutex );

	return SLURM_SUCCESS;
}

void fini(void)
{
	slurm_mutex_lock( &thread_flag_mutex );
	if ( thesis_thread ) {
		verbose( "Thesis scheduler plugin shutting down" );
		stop_thesis_agent();
		pthread_join(thesis_thread, NULL);
		thesis_thread = 0;
	}
	slurm_mutex_unlock( &thread_flag_mutex );
}

int slurm_sched_p_reconfig(void)
{
	thesis_reconfig();
	return SLURM_SUCCESS;
}

uint32_t slurm_sched_p_initial_priority(uint32_t last_prio,
					struct job_record *job_ptr)
{
	return priority_g_set(last_prio, job_ptr);
}
