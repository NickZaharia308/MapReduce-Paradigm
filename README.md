# Map-Reduce Paradigm

## Copyright 2024 Zaharia Nicusor-Alexandru

### Project Idea

The project is based on **Map-Reduce Paradigm**.
As the code is written, it'll be executed in parallel.
Both **Map** and **Reduce** parts will use a specified number of threads to solve the task.

### Tasks descriptions and Workflow

Before starting, the code will run this way:

```bash
./tema1 <noOfMapThreads> <noOfReduceThreads> <inputFile>
```

#### `main():`

`main()` will take care of the arguments, then it will create the mutexes and the barriers that will be later passed in the mappers and in the reducers.

The file parsed as an argument it's opened in `main()` and all its files are pushed in a queue of pairs of strings and ints.
The pairs will represent the file name and the file index.
This queue will be used in the structure for the Map threads.

Following this queue, a shared resource between the two types of threads (Mappers and Reducers), a vector of maps that looks like this, is created:

```cpp
vector<map<string, vector<int>>> wordMaps = vector<map<string, vector<int>>>();
```

The last thing before creating the threads is a vector with the english alphabet characters used in `reduceFunc(void *arg)` to create files starting with each character.

A **single for loop** is used to create both type of threads (Mappers and Reducers).
Before explaining how each type of thread works, the only thing that is done afterwards in `main()` is joining the threads and destroying the mutexes and the barriers.

#### `mapFunc():`

The arguments given from `main()` are casted to the according structure for the Map threads.

The idea for the mapping of the files is that each file starts by processing a file from the queue.
If it's done processing the current file and there are still more files to be processed, it will poll another one.
I preferred a dynamic way of spreading work among threads because it requires less code to be implemented, it's easier to understand, and it's not based on a static heuristic, such as using the files size.

For files processing I used the following approach: read the file line by line, each line is read word by word and a word is filtered in such a manner so only lower case letters from the english alphabet will appear.
Words are stored in a map, where strings (words) are mapped to a vector of ints (A vector and not a simple int is used because the maps will be put in a shared resource between both types of threads).
The map is then added to a vector of maps, a shared resource between Map and Reduce threads.

Both the polling of files (read) and the adding of the created map (write) is used using two separate mutexes.
Before ending the function, a common barrier for both type of threads is placed so that the Reducer threads start their work after all of the Map threads finished theirs.

#### `reduceFunc():`

As in the `mapFunc()`, the arguments are casted and a barrier is place in order to wait all Map threads.

For the Reducers, the idea is that each Reducer will take 2 maps from the vector of maps and merge them into a single one then place the merged map back in the vector of maps until a single map remains.
For merging two maps, for all the words from the second map, it's checked whether or not they appear in the first map. If they do, their indexes are sorted, if they don't a new entry for the word is created in the first map.
The merged map is then pushed back in the vector.

Given that both the reading and writing are performed on the same structure a single mutex is used.

A barrier is placed between the merging of the maps and the writing in the output files, because a thread may process the last 2 maps, while the others will check the condition in the while (**args->wordMaps->size() > 1**), they'll see that it's not true anymore (because the first thread extracted the final two maps to merge them) and they'll move to the next step, without having the final merged map.

For writing in the output files, a simillar dynamic heuristic is used.
If a thread finished writing all the words starting with a letter in an output file, it will try to process the remaining letters.