#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include <queue>
#include <vector>
#include <map>
#include <sstream>
#include <cctype>

using namespace std;

struct mapThreadArgs
{
    int id;
    queue<pair<string, int>> *inputFiles;
    pthread_mutex_t *readFromInputFileMutex;

    vector<map<string, int>> *wordMaps;
    pthread_mutex_t *writeWordMapsMutex;

    pthread_barrier_t *mapReduceBarrier;
};

struct reduceThreadArgs
{
    int id;
    vector<map<string, int>> *wordMaps;
    pthread_mutex_t *readWordMapsMutex;

    map<string, vector<int>> *finalMap;
    pthread_mutex_t *writeFinalMapMutex;

    pthread_barrier_t *mapReduceBarrier;
};

void *reduceFunc(void *arg)
{
    reduceThreadArgs *args = (reduceThreadArgs *)arg;

    // Wait for all map threads to finish
    pthread_barrier_wait(args->mapReduceBarrier);

    // Lock the mutex for reading from the wordMaps for the while loop
    pthread_mutex_lock(args->readWordMapsMutex);

    // If there are 

    pthread_exit(NULL);
}

string filterWord(string& word) {
    string filteredWord;
    for (char c : word) {
        if (isalpha(c)) {
            
            // This will convert to lowercase and keep only characters from the english alphabet
            filteredWord += tolower(c);
        }
    }
    return filteredWord;
}

void *mapFunc(void *arg)
{
    mapThreadArgs *args = (mapThreadArgs *)arg;

    // Lock the mutex for reading from the input files for the while loop
    pthread_mutex_lock(args->readFromInputFileMutex);
    
    while (!args->inputFiles->empty()) {

        // Get the first file from the queue
        pair<string, int> currentPair = args->inputFiles->front();
        args->inputFiles->pop();

        // Unlock the mutex for reading from the input files
        pthread_mutex_unlock(args->readFromInputFileMutex);

        string currentFileName = currentPair.first;
        int currentFileIndex = currentPair.second;

        cout << "Thread " << args->id << " is processing file " <<currentFileName << " with index " << currentFileIndex << endl;


        ifstream currentFile(currentFileName);

        map<string, int> wordMap;
        string line;

        while (getline(currentFile, line)) {
            istringstream lineStream(line);
            string word;

            while (lineStream >> word) {
                string filteredWord = filterWord(word);
                if (!filteredWord.empty()) {
                    wordMap[filteredWord] = currentFileIndex;
                }
            }
        }

        currentFile.close();

        // Put the map in the shared resource map
        pthread_mutex_lock(args->writeWordMapsMutex);
        args->wordMaps->push_back(wordMap);
        pthread_mutex_unlock(args->writeWordMapsMutex);

        // Lock the mutex for reading from the input files for the next while loop
        pthread_mutex_lock(args->readFromInputFileMutex);
    }

    // Unlock the mutex for reading from the input files
    pthread_mutex_unlock(args->readFromInputFileMutex);

    // Before exiting we should wait for all the threads to finish using a barrier
    pthread_barrier_wait(args->mapReduceBarrier);
    
    pthread_exit(NULL);
}

int main(int argc, char **argv)
{
    // The format of the program runs will be: ./tema1 <noOfMapThreads> <noOfReduceThreads> <inputFile>
    if (argc != 4)
    {
        cout << "Wrong format!" << endl;
        cout << "The format should be: ./tema1 <noOfMapThreads> <noOfReduceThreads> <inputFile>" << endl;
        return -1;
    }

    // Getting the number of threads for Map and Reduce
    const int noOfMapThreads = stoi(argv[1]);
    const int noOfReduceThreads = stoi(argv[2]);

    // Getting the name of the input file
    const string inputFileName = argv[3];

    pthread_t mapThreads[noOfMapThreads];
    pthread_t reduceThreads[noOfReduceThreads];

    ifstream inputFile(inputFileName);
    if (!inputFile.is_open())
    {
        cout << "File " << inputFileName  << " could not be opened!" << endl;
        return -1;
    }

    // Create the mutex for reading from the input files
    // We don't want more than one thread to try to access a new input file at the same time
    pthread_mutex_t *readFromInputFileMutex = new pthread_mutex_t;
    pthread_mutex_init(readFromInputFileMutex, NULL);

    // Create the mutex for writing to the shared resource
    // We don't want more than one thread to try to write to the shared resource at the same time
    pthread_mutex_t *writeWordMapsMutex = new pthread_mutex_t;
    pthread_mutex_init(writeWordMapsMutex, NULL);

    // Create the barrier for the threads
    // All Map threads should finish their work before the Reduce threads start, as provided in the task
    pthread_barrier_t *mapReduceBarrier = new pthread_barrier_t;
    pthread_barrier_init(mapReduceBarrier, NULL, noOfMapThreads + noOfReduceThreads);

    // Create the mutex for reading from the wordMaps
    // We don't want more than one thread to try to access the wordMaps and merge them at the same time
    pthread_mutex_t *readWordMapsMutex = new pthread_mutex_t;
    pthread_mutex_init(readWordMapsMutex, NULL);

    // Create the mutex for writing in the final map
    // We don't want more than one thread to try to write to the final map at the same time
    pthread_mutex_t *writeFinalMapMutex = new pthread_mutex_t;
    pthread_mutex_init(writeFinalMapMutex, NULL);

    // The queue of input files, as a pair of the name of the file and the index of the file
    queue<pair<string, int>> *inputFiles = new queue<pair<string, int>>();

    // First line is the number of files
    string firstLine;
    getline(inputFile, firstLine);
    int noOfFiles = stoi(firstLine);

    // Reading the files and adding them in the queue
    for (int i = 1; i <= noOfFiles; i++)
    {
        string nameOfTheCurrentFile;
        inputFile >> nameOfTheCurrentFile;
        inputFiles->push(make_pair(nameOfTheCurrentFile, i));    
    }

    // Creating the shared resource between the 2 types of threads
    vector<map<string, int>> wordMaps = vector<map<string, int>>(0);

    for (int i = 0; i < noOfMapThreads + noOfReduceThreads; i++)
    {
        if (i < noOfMapThreads) {

            mapThreadArgs *args = new mapThreadArgs();
            args->id = i;
            args->readFromInputFileMutex = readFromInputFileMutex;

            args->inputFiles = inputFiles;
            args->wordMaps = &wordMaps;
            args->writeWordMapsMutex = writeWordMapsMutex;

            int r = pthread_create(&mapThreads[i], NULL, mapFunc, args);

            if (r) {
                cout << "Error creating thread " << i << endl;
                return -1;
            }
        } else {
            reduceThreadArgs *args = new reduceThreadArgs();
            args->id = i;
            args->readWordMapsMutex = readWordMapsMutex;
            args->wordMaps = &wordMaps;

            map<string, vector<int>> *finalMap = new map<string, vector<int>>();
            args->finalMap = finalMap;
            args->writeFinalMapMutex = writeFinalMapMutex;

            args->mapReduceBarrier = mapReduceBarrier;

            int r = pthread_create(&reduceThreads[i - noOfMapThreads], NULL, reduceFunc, args);

            if (r) {
                cout << "Error creating thread " << i << endl;
                return -1;
            }
        }
    }

    void *status;
    for (int i = 0; i < noOfMapThreads; i++) {
		int r = pthread_join(mapThreads[i], &status);

		if (r) {
			cout << "Error joining thread " << i << endl;
            return -1;
		}
	}

    // Destroy the mutexes
    pthread_mutex_destroy(readFromInputFileMutex);
    pthread_mutex_destroy(writeWordMapsMutex);
    pthread_mutex_destroy(readWordMapsMutex);
    pthread_mutex_destroy(writeFinalMapMutex);

    // Destroy the barrier
    pthread_barrier_destroy(mapReduceBarrier);

    // Write all the values from wordMaps
    for (auto it = wordMaps.begin(); it != wordMaps.end(); it++) {
        for (auto it2 = it->begin(); it2 != it->end(); it2++) {
            cout << it2->first << " " << it2->second << endl;
        }
    }

    return 0;
}