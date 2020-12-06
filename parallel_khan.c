#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <time.h>
#include <sys/time.h>
int rows,cols,edges,exit_counter=0,S_size=0; 								//global vars for reading the file 
int ** graph;														// adjacency matrix 
int * incoming_edges;												// array of incoming degree
struct timeval tstruct1;											// the rest is for the list L and timing the program
struct timeval tstruct2;
long unsigned passed_time;
time_t t1,t2;
typedef struct node
{
	int node_id;
	struct node * next;
}node;
node * temp = NULL;
node * list = NULL;
node * L_tail=NULL;
node * Set=NULL;
node * S_tail=NULL;
node * local_S=NULL;
node * local_S_tail=NULL;
node * newnode=NULL;
void S_append();
void S_iterate();
void L_append();
void L_iterate();
int S_pop();
void process();
omp_lock_t masterSlock;
omp_lock_t TailSlock;												//lock to be used for securing the node * Set var in pop and append
int main(int argc, char const *argv[])
{
	
	if(argc != 3)													// printing usage of program, to be used with a file from dag.c
	{
		printf("Usage : ./program graph.txt Number_of_threads\n");
		exit(0);
	}
	
	FILE * ptr;														//file pointer to read from
	int * ptr2;														//pointer for the initialization of graph(adjacency matrix) array
	
	int i,len;
	ptr = fopen(argv[1],"r");										//opening the file to read from
	if (ptr == NULL)
	{
		printf("Couldn't open the file\n");
		exit(-1);
	}
	omp_set_num_threads(atoi(argv[2]));									//set number of threads to be used
	fscanf(ptr,"%d %d %d",&rows,&cols,&edges);							//reading first three vars to get adjacency matrix size
	len = sizeof(int *) * rows + sizeof(int) * cols * rows; 			// allocating space for the 2d array
    graph = (int **)malloc(len); 
    																	// ptr is now pointing to the first element in of 2D array 
    ptr2 = (int *)(graph + rows); 
    																	// for loop to point rows pointer to appropriate location in 2D array 
    for(i = 0; i < rows; i++)
    { 
        graph[i] = (ptr2 + cols * i);
	}
	incoming_edges = (int *) malloc(rows * sizeof(int));				// mallocing for the array of incoming deg array
	if (incoming_edges == NULL)
	{
		printf("Could't allocate space for incoming edges\n");
	}
	
	
	
	
	for (int i = 0; i < rows; i++)										// init of adjacency matrix
	{
		for (int j = 0; j < cols; j++)
		{
			graph[i][j] = 0;
		}
	}
	
	int from,to;														//begin reading the file and fill the adjacency matrix
	for (int i = 0; i < edges; ++i)
	{
		
		from = 0;
		to = 0;
		fscanf(ptr,"%d %d",&from,&to);
		graph[from][to]=1;
	}
	
	
	
	for (int i = 0; i < rows; i++)										// counting the 1's in each column to get indegree of each node
	{
		int count=0; 
		for (int j = 0; j < cols; j++)
		{
			if (graph[j][i]==1)
			{
				count++;
			}
		}
		incoming_edges[i] = count;
	}
	int node;
	int sync;
	printf("did the prep,starting timer\n");
	gettimeofday(&tstruct1,NULL);										//starting timer
	t1 = tstruct1.tv_usec ;
	t1 /= 1000;
	t1 += tstruct1.tv_sec * 1000;
	omp_init_lock(&masterSlock);
	omp_init_lock(&TailSlock);
	
	#pragma omp parallel 
	{

		#pragma omp for schedule(dynamic)
		for (int i = 0; i < rows; ++i)									//parallely append the first batch of nodes 
		{
			if (incoming_edges[i]==0)
			{
				S_append(i);											//no need for critical, done inside the append func
			}
		}										
		#pragma omp single
		{
			if (S_size==0)
			{
				printf("The graph is cyclic...exiting...\n");
				exit(-1);
			}
		}
		#pragma omp barrier
		#pragma omp master
		{

			while(1)													//exit condition is inside the L_append() method
			{
				
				while(S_size>0)											//while there are elements to pop, pop and create a task to process them
				{
					
					
					temp = NULL;
					
					omp_set_lock(&masterSlock); 
					
					if (Set==NULL)
					{
						Set = S_tail;
					}
					temp = Set;
					
					Set = temp->next;
					
					omp_unset_lock(&masterSlock);
													//unlocking
					#pragma omp atomic
						S_size--;
					
					node = temp->node_id;
					
					#pragma omp task default(none) firstprivate(node,local_S,local_S_tail,newnode) shared(Set,S_tail,rows)
					{
																		//tasking node 
						
						process(node,local_S,local_S_tail,newnode);
						
					}
					
				}
				
				#pragma omp taskwait
			}
		}
	}
}

	

void process(int node_id,node * local_S,node * local_S_tail,node * newnode)
{
		
		//333333333333333333333333333333333333333333333333333333333333333333333333333333333333
		newnode=NULL;

		#pragma omp atomic
		exit_counter++;														//atomically update the counter to exit the program
		
	
		#pragma omp critical (mallocking)
		{
			newnode = (node *)malloc(sizeof(node));
		}
		newnode->node_id=node_id;
		newnode->next = NULL;
		
		#pragma omp critical (appending)
		{
		
			if (list == NULL)											//if list == null then it's an empty list
			{
				list = newnode;										//append the first node
				L_tail = list;
			}
			else
			{

				L_tail->next = newnode;									//if not empty list then keep a tail pointer, add the new node and
				L_tail = L_tail->next;										//update the tail
			}
			
		

			if (exit_counter==rows)										//if all the nodes have been appended time it, call iterate() 
			{															//which prints the list L print time passed since start of 
				gettimeofday(&tstruct2,NULL);							//parallel and exit 
				t2 = tstruct2.tv_usec;
				t2 /= 1000;
				t2 += tstruct2.tv_sec * 1000;
				
				passed_time = (t2 - t1);
				
				L_iterate();
				printf("Elapsed Time in Parallel Section: %lu\n",passed_time);
				//#printf("t2 : %lu \nt1: %lu \n",t2,t1);
				exit(0);
				
			}
		}
		//333333333333333333333333333333333333333333333333333333333333333333333333333333333333
		
		local_S=NULL;
		local_S_tail=NULL;
		newnode=NULL;
		
		int counter=0;
	for (int i = 0; i < rows; i++)
	{
		
		if (graph[node_id][i]==1)
		{
																//deleting outgoing edge 
			
			graph[node_id][i]=0;  								//no critical needed here, every thread accesses different locations
			
																//atomically updating indegree with i
			#pragma omp atomic
				incoming_edges[i]--;
			
			if (incoming_edges[i]==0)
			{
															//the node i has a 0 indegree appending to S
				
				
				#pragma omp critical (mallocking)
				{
					newnode = (node *)malloc(sizeof(node));
				}
				
				if (newnode==NULL)
				{
					printf("new node not created\n");
				}
				
				newnode->node_id=i;
				newnode->next = NULL;
				
				if (local_S == NULL)											//if Set == null then it's an empty Set
				{
					
					local_S = newnode;										//append the first node
					local_S_tail = local_S;
					
				}
				else
				{
					
					local_S_tail->next = newnode;									//if not empty list then keep a tail pointer, add the new node and
					local_S_tail = local_S_tail->next;										//update the tail
				}
				counter++;				
			}					
		}
	}
	if (local_S!=NULL)
	{
		
		#pragma omp critical (localtoglobal)
		{
			
			#pragma omp atomic
				S_size+=counter;
			
			if (Set==NULL)
			{	omp_set_lock(&masterSlock);
				Set=local_S;
				omp_unset_lock(&masterSlock);
			}
			else
			{
				S_tail->next=local_S;				
			}
			S_tail = local_S_tail;
			
		}
	}
	
}




	
	

void L_append(int id)
{
	
	#pragma omp atomic
		exit_counter++;														//atomically update the counter to exit the program
	
	
	#pragma omp critical (mallocking)
	{
		newnode = (node *)malloc(sizeof(node));
	}
	newnode->node_id=id;
	newnode->next = NULL;
	
	#pragma omp critical (appending)
	{
	
		if (list == NULL)											//if list == null then it's an empty list
		{
			list = newnode;										//append the first node
			L_tail = list;
		}
		else
		{

			L_tail->next = newnode;									//if not empty list then keep a tail pointer, add the new node and
			L_tail = L_tail->next;										//update the tail
		}
		
	

		if (exit_counter==rows)										//if all the nodes have been appended time it, call iterate() 
		{															//which prints the list L print time passed since start of 
			gettimeofday(&tstruct2,NULL);							//parallel and exit 
			t2 = tstruct2.tv_usec;
			t2 /= 1000;
			t2 += tstruct2.tv_sec * 1000;
			
			passed_time = (t2 - t1);
			
			L_iterate();
			printf("Elapsed Time in Parallel Section: %lu\n",passed_time);
			
			exit(0);
			
		}
	}
}

void L_iterate()														//iterate through the next pointers and print the node_id
{
	node * p = list;
	while(p!=NULL)
	{
		printf("%d\n",p->node_id);
		p = p->next;

	}
}




void S_append(int id)
{
	#pragma omp atomic
	S_size++;														//atomically update the counter to exit the program
	node * newnode;
	#pragma omp critical (mallocking)
	{																//create new node and initialise it
		newnode = (node *)malloc(sizeof(node));
	}
	if (newnode==NULL)
	{
		printf("new node not created\n");
	}
	
	newnode->node_id=id;
	newnode->next = NULL;
	
	#pragma omp critical (appending)
	{
	
		if (Set == NULL)											//if Set == null then it's an empty Set
		{
			omp_set_lock(&masterSlock);
			Set = newnode;
													//append the first node
			S_tail = Set;
			omp_unset_lock(&masterSlock);
		}
		else
		{
			
			S_tail->next = newnode;									//if not empty list then keep a tail pointer, add the new node and
			S_tail = S_tail->next;										//update the tail
		}
	}
}

void S_iterate()														//iterate through the next pointers and print the node_id
{
	node * p = Set;
	while(p!=NULL)
	{
		printf("%d\n",p->node_id);
		p = p->next;

	}
}


int S_pop()
{
	
	
	node * temp = NULL;
	
	omp_set_lock(&masterSlock); 
	
	if (Set==NULL)
	{
		Set = S_tail;
	}
	temp = Set;
	
	Set = temp->next;
	
	omp_unset_lock(&masterSlock);
										//unlocking
	#pragma omp atomic
		S_size--;
	
	return temp->node_id;
}