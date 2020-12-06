#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define MIN_PER_RANK	2	/* Nodes/Rank: How 'fat' the DAG should be.  */
#define MAX_PER_RANK	7
#define PERCENT		10	/* Chance of having an Edge.  */

int main(int argc, char *argv[])
{
	int	i, j, k, nodes = 0, edges = 0;
	int	ranks = atoi(argv[1]);
	char	command[256];
	FILE	*outfile;

	srand(time(NULL));

	outfile = fopen("dag.txt", "w");

	for (i = 0; i < ranks; i++) {
		/* New nodes of 'higher' rank than all nodes generated till now.  */
		int new_nodes = MIN_PER_RANK + (rand () % (MAX_PER_RANK - MIN_PER_RANK + 1));

		/* Edges from old nodes ('nodes') to new ones ('new_nodes').  */
		for (j = 0; j < nodes; j++) {
			for (k = 0; k < new_nodes; k++) {
				if ((rand() % 100) < PERCENT) {
					fprintf(outfile, "%d %d\n", j, k + nodes);
					edges++;
				}
			}
		}

		nodes += new_nodes; /* Accumulate into old node set.  */
	}

	fclose(outfile);

	sprintf(command, "sed -i '1s/^/%d %d %d\\n/' dag.txt", nodes, nodes, edges);

	system(command);

	return 0;
}