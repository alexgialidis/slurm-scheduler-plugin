/*****************************************************************************\
 *  thesis.h - header for alex's thesis scheduler plugin.
\*****************************************************************************/

#ifndef _SLURM_THESIS_H
#define _SLURM_THESIS_H

/* thesis_agent - detached thread periodically when pending jobs can start */
extern void *thesis_agent(void *args);

/* Terminate thesis_agent */
extern void stop_thesis_agent(void);

/* Note that slurm.conf has changed */
extern void thesis_reconfig(void);

#endif	/* _SLURM_THESIS_H */
