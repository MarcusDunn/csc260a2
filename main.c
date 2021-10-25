#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>


#define NUM_CLERKS 5
/**
 * a global variable marking the total number of customers that will be served today.
 * The ONLY place this should be set is ONCE inside of acs_parse_input, otherwise it should be treated as a compile
 * time constant.
 */
long total_number_of_customers = 0;

/**
 * simple enum over the two classes of customers
 */
typedef enum Class {
    Economy = 0,
    Business = 1,
} Class;

/**
 * contains data about the customer
 */
typedef struct Customer {
    long id;
    Class class;
    long arrival_time_millis;
    long service_time_millis;
} Customer;

/**
 * The backbone of the operation. there are two concurrent queues here one for business and one for economy.
 * As Clerks must check business and then (upon failure to get a business customer) try to get a economy customer,
 * we lock reading and writing to the following under queues_mutex: econ_queue, econ_queue_head, econ_queue_tail,
 * buis_queue, buis_queue_head, buis_queue_tail. This could be done quicker if we seperated mutexes a bit (one for
 * business and one for econ) but for the sake of simple waking logic (and 2 mutexes makes deadlocks trivial) I used one.
 * The queues_condvar wakes threads when there are customers in Queue. This waking is only done by the main thread.
 */
typedef struct SyncQueues {
    Customer **econ_queue;
    int econ_queue_head;
    int econ_queue_tail;
    Customer **buis_queue;
    int buis_queue_head;
    int buis_queue_tail;
    pthread_mutex_t *queues_mutex;
    pthread_cond_t *queues_condvar;
} SyncQueues;

/**
 * this is essentially clerk record keeping and access to the shared SyncQueues. clerk_id is for printing nice numbers,
 * ait_time_ledger is for registering wait times for buisness and economy customers, and performance_numbers is to
 * record how many customers were served. this wait_time_ledger and performance_numbers is mutably borrowed by the clerk
 * thread, it is not safe to access until its corresponding thread is joined.
 */
typedef struct ClerkInfo {
    SyncQueues *sync_queues;
    long long int wait_time_ledger[2];
    int performance_numbers[2];
    int clerk_id;
} ClerkInfo;

/**
 * uses in qsort of customers as a comparison functions
 * @param c1 customer 1
 * @param c2 customer 2
 * @return weather c1 arrived earlier than c2, comparing id's in the case of equality (total orderings!).
 */
static int acs_cmp_customers_by_arrival_time(const void *c1, const void *c2);

/**
 * give a heap allocated dump of the contexts of argv[1] with some handy error checking!
 * @param argc num args
 * @param argv args
 * @returns heap allocated dump of the contexts of argv[1]. MAKE SURE YOU FREE IT.
 */
char *acs_get_file_from_args(int argc, char *argv[]);

/**
 * parses a single line of input into a single customer. exits early with an error message on invalid input;
 * @param line
 * @returns a single customer
 */
Customer *acs_parse_line(char *line);

/**
 * malloc but yells loudly and dies if something goes wrong
 * @param size the size of memory you wish to allocate
 * @returns whatever you want it to be :)
 */
void *acs_e_alloc(size_t size);

/**
 * reads a *valid* readable text file to a string. this will do nasty things if given something like stdin or
 * /dev/urandom. Don't.
 * @param file a valid readable text file
 * @returns a the contents of said file
 */
char *acs_read_to_string(FILE *file);

/**
 * calls qsort on customers using acs_cmp_customers_by_arrival_time
 * @param customers the customers to sort
 */
void acs_sort_customers_by_arrival_time_ascending(Customer **customers);

/**
 * sets up the threads and runs the simulation
 * @param customers the customers to simulate
 */
void acs_run_sim(Customer **customers);

/**
 * the worker threads!
 * @param arg a ClerkInfo for this clerk
 */
void *acs_clerk_worker(void *arg);

/**
 * helper function for creating SyncQueues without much ceremony
 * @param mutex the mutex guarding the properties of SyncQueues
 * @param condvar the condvar notifying of pushes to the two SyncQueues
 * @returns a well formed SyncQueues with logical defaults
 */
SyncQueues acs_init_sync_queue(pthread_mutex_t *mutex, pthread_cond_t *condvar);

/**
 * the e_pthread functions all yell loudly and die if the underlying pthread function returns anything unexpected.
 */
void acs_e_pthread_mutex_lock(pthread_mutex_t *mutex);

/**
 * the e_pthread functions all yell loudly and die if the underlying pthread function returns anything unexpected.
 */
void acs_e_pthread_cond_signal(pthread_cond_t *cond);

/**
 * the e_pthread functions all yell loudly and die if the underlying pthread function returns anything unexpected.
 */
void acs_e_pthread_mutex_unlock(pthread_mutex_t *mutex);

/**
 * the e_pthread functions all yell loudly and die if the underlying pthread function returns anything unexpected.
 */
void acs_e_pthread_cond_wait(pthread_cond_t *condvar, pthread_mutex_t *mutex);

/**
 * gets a timestamp for right now in milliseconds. Only useful if compared to another timestamp.
 * @returns a timestamp for right now measured in milliseconds
 */
long long int acs_get_timestamp_millis();

/**
 * pretty prints the stats from the simulation.
 * @param business_wait_time_millis tricks in the name!
 * @param economy_wait_time_millis tricks in the name!
 * @param business_customers_served tricks in the name!
 * @param economy_customers_served tricks in the name!
 */
void acs_print_stats(long long int business_wait_time_millis, long long int economy_wait_time_millis,
                     int business_customers_served, int economy_customers_served);

/**
 * parses the input into an array of pointers to Customers
 * @param input the input to the program as described in the assignment description
 * @returns an array of customers. Must be freed in addition to each customer;
 */
Customer **acs_parse_input(char *input);

/**
 * frees a dynamically allocated array of customers (and its contents) of length total_number_of_customers.
 * @param customers the array to free!
 */
void acs_free_customer_array(Customer **customers);

void *customer_timer(void *arg);

/**
 * entry point, expects a single argument to a correctly formatted text file
 * @return 0 on success. an error message and 1 on failure.
 */
int main(int argc, char *argv[]) {
    char *contents = acs_get_file_from_args(argc, argv);

    Customer **customers = acs_parse_input(contents);

    free(contents);

    // sort in case that the input file was not properly sorted by arrival time.
    acs_sort_customers_by_arrival_time_ascending(customers);

    acs_run_sim(customers);

    acs_free_customer_array(customers);

    return 0;
}

void *acs_clerk_worker(void *arg) {
    long long int start_time_milliseconds = acs_get_timestamp_millis();
    ClerkInfo *clerkInfo = (ClerkInfo *) arg;
    SyncQueues *syncQueues = clerkInfo->sync_queues;
    int id = clerkInfo->clerk_id;
    printf("clerk %d started working!\n", id);
    for (;;) {
    	acs_e_pthread_mutex_lock(syncQueues->queues_mutex);
        while (syncQueues->econ_queue_tail == syncQueues->econ_queue_head &&
               syncQueues->buis_queue_tail == syncQueues->buis_queue_head) {
            acs_e_pthread_cond_wait(syncQueues->queues_condvar, syncQueues->queues_mutex);
        }
        Customer *customer;
        if (syncQueues->buis_queue_tail < syncQueues->buis_queue_head) {
            customer = syncQueues->buis_queue[syncQueues->buis_queue_tail];
            syncQueues->buis_queue_tail++;
        } else if (syncQueues->econ_queue_tail < syncQueues->econ_queue_head) {
            customer = syncQueues->econ_queue[syncQueues->econ_queue_tail];
            syncQueues->econ_queue_tail++;
        } else {
            printf("unexpected things have occurred in %d!!\n", id);
            exit(1);
        }
        acs_e_pthread_mutex_unlock(syncQueues->queues_mutex);


        printf("clerk %d has started taking care of customer %ld\n", id, customer->id);
        // at this point we've safely obtained sole ownership of customer;

        // calc how long the customer has waited and add it to the wait_time_ledger
        long long int current_time_millis = acs_get_timestamp_millis();
        long long int time_elapsed_millis = current_time_millis - start_time_milliseconds;
        long long int wait_time = time_elapsed_millis - customer->arrival_time_millis;
        printf("customer %ld (%s) spent %.2f seconds waiting before being severed\n",
               customer->id, customer->class == Business ? "Business" : "Economy", (double) wait_time / 1000.0);
        clerkInfo->wait_time_ledger[customer->class] += wait_time;
        clerkInfo->performance_numbers[customer->class]++; // gotta make sure boss knows we're working!

        errno = 0;
        if (usleep(customer->service_time_millis * 1000) != 0) {
            printf("usleep failed with %d", errno);
            exit(1);
        }
        printf("after %.2f seconds clerk %d has finished taking care of customer %ld",
               ((float) customer->service_time_millis / 1000.0), id, customer->id);

        acs_e_pthread_mutex_lock(syncQueues->queues_mutex);
        if (syncQueues->econ_queue_tail + syncQueues->buis_queue_tail == total_number_of_customers) {
            acs_e_pthread_mutex_unlock(syncQueues->queues_mutex);
            printf(" which was their final customer\n");
            pthread_exit(0);
        } else {
            acs_e_pthread_mutex_unlock(syncQueues->queues_mutex);
        }
        printf("\n");
    }
}

long long int acs_get_timestamp_millis() {
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) {
        printf("C literally wouldn't give us the time of day!\n");
        exit(1);
    }
    // big boi
    long long int milliseconds = tv.tv_sec * 1000LL + tv.tv_usec / 1000;
    return milliseconds;
}

void acs_e_pthread_cond_wait(pthread_cond_t *condvar, pthread_mutex_t *mutex) {
    if (pthread_cond_wait(condvar, mutex) != 0) {
        printf("failed to pthread_cond_wait");
        exit(1);
    }
}

typedef struct CustomerInfo {
    Customer *customer;
    SyncQueues *syncQueues;
} CustomerInfo;

void acs_run_sim(Customer **customers) {
    pthread_mutex_t sync_queue_mutex;
    pthread_cond_t sync_queue_condvar;
    pthread_mutex_init(&sync_queue_mutex, NULL);
    pthread_cond_init(&sync_queue_condvar, NULL);
    SyncQueues sync_queues = acs_init_sync_queue(&sync_queue_mutex, &sync_queue_condvar);

    pthread_t clerk_threads[NUM_CLERKS];
    ClerkInfo clerks[5];
    for (int i = 0; i < NUM_CLERKS; ++i) {
        clerks[i].sync_queues = &sync_queues;
        clerks[i].clerk_id = i;
        clerks[i].wait_time_ledger[Business] = 0;
        clerks[i].wait_time_ledger[Economy] = 0;
        clerks[i].performance_numbers[Business] = 0;
        clerks[i].performance_numbers[Economy] = 0;
        if (pthread_create(&clerk_threads[i], NULL, &acs_clerk_worker, &clerks[i]) != 0) {
            printf("failed to create thread\n");
            exit(1);
        }
    }

    // Simulation actually starts here
    long long int start_time = acs_get_timestamp_millis();

    pthread_t customer_threads[total_number_of_customers];
    CustomerInfo customer_info[total_number_of_customers];
    for (int i = 0; i < total_number_of_customers; ++i) {
        Customer *customer = customers[i];
        customer_info[i].syncQueues = &sync_queues;
        customer_info[i].customer = customer;
        pthread_create(&customer_threads[i], NULL, customer_timer, &customer_info[i]);
    }

    for (int i = 0; i < total_number_of_customers; ++i) {
        int bad_exit = -1;
        int *thread_return = &bad_exit;
        int pthread_join_result = pthread_join(customer_threads[i],  (void **) &thread_return);
        if (pthread_join_result != 0 || thread_return != 0) {
            printf("something went poorly when exiting customer thread %d: %d, %d\n", i, pthread_join_result, thread_return);
        } else {
            printf("successfully exited customer thread %d\n", i);
        }
    }

    long long int business_wait_time_millis = 0;
    long long int economy_wait_time_millis = 0;
    int business_customers_served = 0;
    int economy_customers_served = 0;

    for (int i = 0; i < NUM_CLERKS; ++i) {
        int bad_exit = -1;
        int *thread_return = &bad_exit;
        int pthread_join_result = pthread_join(clerk_threads[i], (void **) &thread_return);
        if (pthread_join_result != 0 || thread_return != 0) {
            printf("something went poorly when exiting clerk thread %d: %d, %d\n", clerks[i].clerk_id, pthread_join_result, thread_return);
        } else {
            printf("successfully exited thread %d\n", clerks[i].clerk_id);
        }

        // these are safe to access as the only thread that modifies them we just joined!
        business_wait_time_millis += clerks[i].wait_time_ledger[Business];
        economy_wait_time_millis += clerks[i].wait_time_ledger[Economy];
        business_customers_served += clerks[i].performance_numbers[Business];
        economy_customers_served += clerks[i].performance_numbers[Economy];
    }

    printf(
            "\n--------------------------------------------\n"
            "over the course of a %lld second simulation:"
            "\n--------------------------------------------\n",
            (acs_get_timestamp_millis() - start_time) / 1000);

    acs_print_stats(business_wait_time_millis, economy_wait_time_millis, business_customers_served,
                    economy_customers_served);


    int condDestroy = pthread_cond_destroy(&sync_queue_condvar);
    if (condDestroy != 0) {
        printf("error %d in destroying sync_queue_condvar, continuing nonetheless\n", condDestroy);
    }
    int mutexDestroy = pthread_mutex_destroy(&sync_queue_mutex);
    if (mutexDestroy != 0) {
        printf("error %d in destroying sync_queue_mutex, continuing nonetheless\n", mutexDestroy);
    }
    free(sync_queues.econ_queue);
    free(sync_queues.buis_queue);
}

void *customer_timer(void *arg) {
    CustomerInfo *customer_info = (CustomerInfo *) arg;
    Customer *customer = customer_info->customer;
    usleep(customer_info->customer->arrival_time_millis * 1000);
    printf("customer %ld has entered the %s queue\n", customer->id,
           customer->class == Business ? "Business" : "Economy");
    SyncQueues *sync_queues = customer_info->syncQueues;
    acs_e_pthread_mutex_lock(sync_queues->queues_mutex);
    if (customer->class == Business) {
        (*sync_queues).buis_queue[(*sync_queues).buis_queue_head++] = customer;
    } else if (customer->class == Economy) {
        (*sync_queues).econ_queue[(*sync_queues).econ_queue_head++] = customer;
    } else {
        printf("Invalid customer class for customer %ld\n", customer->id);
        exit(1); // let OS handle this mess :)
    }
    acs_e_pthread_cond_signal(sync_queues->queues_condvar);
    // ready for someone to grab a customer!
    acs_e_pthread_mutex_unlock(sync_queues->queues_mutex);
    pthread_exit(0);
}

void acs_print_stats(long long int business_wait_time_millis, long long int economy_wait_time_millis,
                     int business_customers_served, int economy_customers_served) {
    int total_customers_served = business_customers_served + economy_customers_served;
    if (total_customers_served != total_number_of_customers) {
        printf("something has gone horrid!\n");
        exit(1);
    }
    long long total_wait_time = business_wait_time_millis + economy_wait_time_millis;
    printf("We served %d customers, of which %d were business and %d were economy!\n\n", total_customers_served,
           business_customers_served, economy_customers_served);
    printf("Customers spent a total of %lld seconds waiting!\nThe average waiting time for all customers is: %.2f seconds. \n\n",
           total_wait_time / 1000,
           ((float) total_wait_time / (float) total_customers_served) / 1000);
    printf("Business-class customers spent a total of %lld seconds waiting!\nThe average waiting time for all business-class customers is: %.2f seconds. \n\n",
           business_wait_time_millis / 1000,
           ((float) business_wait_time_millis / (float) business_customers_served) / 1000);
    printf("Economy-class customers spent a total of %lld seconds waiting!\nThe average waiting time for all economy-class customers is: %.2f seconds. \n\n",
           economy_wait_time_millis / 1000,
           ((float) economy_wait_time_millis / (float) economy_customers_served) / 1000);
}

void acs_e_pthread_mutex_unlock(pthread_mutex_t *mutex) {
    if (pthread_mutex_unlock(mutex) != 0) {
        printf("error when signaling sync_queue_condvar\n");
        exit(1);
    }
}

void acs_e_pthread_cond_signal(pthread_cond_t *cond) {
    if (pthread_cond_signal(cond) != 0) {
        printf("error when signaling sync_queue_condvar\n");
        exit(1);
    }
}

void acs_e_pthread_mutex_lock(pthread_mutex_t *mutex) {
    if (pthread_mutex_lock(mutex) != 0) {
        printf("failed to lock mutex\n");
        exit(1);
    }
}

SyncQueues acs_init_sync_queue(pthread_mutex_t *mutex, pthread_cond_t *condvar) {
    SyncQueues syncQueues;
    // these two could be shared with some fancy indexing at different ends of this chunk of memory, but I'll keep it simple
    syncQueues.econ_queue = (Customer **) malloc(sizeof(Customer *) * total_number_of_customers);
    syncQueues.buis_queue = (Customer **) malloc(sizeof(Customer *) * total_number_of_customers);
    syncQueues.buis_queue_tail = 0;
    syncQueues.econ_queue_tail = 0;
    syncQueues.buis_queue_head = 0;
    syncQueues.econ_queue_head = 0;
    syncQueues.queues_condvar = condvar;
    syncQueues.queues_mutex = mutex;
    return syncQueues;
}

void acs_sort_customers_by_arrival_time_ascending(Customer **customers) {
    qsort(customers, total_number_of_customers, sizeof(Customer *), acs_cmp_customers_by_arrival_time);
}

void *acs_e_alloc(size_t size) {
    void *mem = malloc(size);
    if (mem == NULL) {
        printf("failed to alloc %zu bytes\n", size);
        exit(1);
    }
    return mem;
}

char *acs_get_file_from_args(int argc, char **argv) {
    if (argc != 2) {
        printf("incorrect number of arguments\n");
        exit(1);
    } else {
        char *file_name = argv[1];
        errno = 0;
        FILE *file = fopen(file_name, "r");
        if (file != NULL) {
            return acs_read_to_string(file);
        } else {
            printf("failed to open file \"%s\" with %d\n", file_name, errno);
            exit(1);
        }
    }
}

char *acs_read_to_string(FILE *file) {
    fseek(file, 0L, SEEK_END);
    long bytes = ftell(file);
    fseek(file, 0L, SEEK_SET);
    char *contents = (char *) acs_e_alloc(bytes * sizeof(char));
    fread(contents, sizeof(char), bytes, file);
    return contents;
}

Customer *acs_parse_line(char *line) {
    char unmodified_line[1000] = {0};
    strcpy(unmodified_line, line);
    errno = 0;
    char *save_ptr, *end_ptr = NULL;
    char *id_string = strtok_r(line, ":", &save_ptr);
    long id = strtol(id_string, &end_ptr, 10);
    if (errno == 0 && end_ptr != id_string) {
        end_ptr = NULL;
        char *class_string = strtok_r(NULL, ",", &save_ptr);
        long class = strtol(class_string, &end_ptr, 10);
        if (errno == 0 && end_ptr != class_string && (class == Business || class == Economy)) {
            end_ptr = NULL;
            char *arrival_time_string = strtok_r(NULL, ",", &save_ptr);
            long arrival_time = strtol(arrival_time_string, &end_ptr, 10);
            if (errno == 0 && end_ptr != arrival_time_string) {
                end_ptr = NULL;
                char *service_time_string = strtok_r(NULL, "", &save_ptr);
                long service_time = strtol(service_time_string, &end_ptr, 10);
                if (errno == 0 && end_ptr != service_time_string) {
                    end_ptr = NULL;
                    Customer *customer = acs_e_alloc(sizeof(Customer));
                    customer->id = id;
                    customer->class = class;
                    customer->arrival_time_millis = arrival_time * 100;
                    customer->service_time_millis = service_time * 100;
                    return customer;
                } else {
                    printf("error parsing service_time_millis from line \"%s\"\n", unmodified_line);
                }
            } else {
                printf("error parsing arrival_time_millis from line \"%s\"\n", unmodified_line);
            }
        } else {
            printf("error parsing class from line \"%s\" and class_string \"%s\"\n", unmodified_line, class_string);
        }
    } else {
        printf("error parsing id from line \"%s\" and \n", unmodified_line);
    }
    return NULL;
}

int acs_cmp_customers_by_arrival_time(const void *c1, const void *c2) {
    Customer **pCustomer1 = (Customer **) c1;
    Customer **pCustomer2 = (Customer **) c2;
    long arrivalTime1 = (*pCustomer1)->arrival_time_millis;
    long arrivalTime2 = (*pCustomer2)->arrival_time_millis;
    if (arrivalTime1 == arrivalTime2) {
        return ((*pCustomer1)->id > (*pCustomer2)->id);
    } else {
        return arrivalTime1 > arrivalTime2;
    }
}

Customer **acs_parse_input(char *input) {
    char *save_ptr, *end_ptr = NULL;
    char *customer_number_string = strtok_r(input, "\n", &save_ptr);
    errno = 0;
    total_number_of_customers = strtol(customer_number_string, &end_ptr, 10);
    if (errno == 0 && end_ptr != customer_number_string) {
        // nice little array of pointers to customers, we love C.
        Customer **customers = (Customer **) acs_e_alloc(sizeof(Customer *) * total_number_of_customers);
        char *line = NULL;
        int i = 0;
        while ((line = strtok_r(NULL, "\n", &save_ptr)) != NULL && i < total_number_of_customers) {
            Customer *customer = acs_parse_line(line);
            if (customer == NULL) {
                printf("failed to parse a customer %s\n", line);
                exit(1);
            } else {
                customers[i] = customer;
                i++;
            }
        }
        if (i != total_number_of_customers) {
            printf("unexpected number of customers. the file said there was %ld but only found %d",
                   total_number_of_customers, i);
            exit(1);
        } else {
            return customers;
        }

    } else {
        printf("invalid input file. First line must consist of a single number denoting the number of customers\n");
        exit(1);
    }
}

void acs_free_customer_array(Customer **customers) {
    for (int i = 0; i < total_number_of_customers; ++i) {
        free(customers[i]);
    }
    free(customers);
}
