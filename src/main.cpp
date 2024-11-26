#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include <queue>
#include <vector>
#include <map>
#include <sstream>
#include <cctype>
#include <algorithm>

using namespace std;

struct mapThreadArgs
{
    int id;
    queue<pair<string, int>> *inputFiles;
    pthread_mutex_t *readFromInputFileMutex;

    vector<map<string, vector<int>>> *wordMaps;
    pthread_mutex_t *writeWordMapsMutex;

    pthread_barrier_t *mapReduceBarrier;
};

struct reduceThreadArgs
{
    int id;
    vector<map<string, vector<int>>> *wordMaps;
    pthread_mutex_t *readWriteWordMapsMutex;

    pthread_barrier_t *mapReduceBarrier;

    pthread_mutex_t *writeOutputFilesMutex;
    vector<char> *outputChars;

    pthread_barrier_t *reduceBarrier;
};

vector<pair<string, vector<int>>> getKeysStartingWith(map<string, vector<int>>& wordMap, char letter) {
    // Check if wordMap is empty
    if (wordMap.empty()) {
        return vector<pair<string, vector<int>>>();
    }

    vector<pair<string, vector<int>>> keyValues;

    // Extract the key-value pairs that start with the given letter
    for (const auto& keyValue : wordMap) {
        if (keyValue.first[0] == letter) {
            keyValues.push_back(keyValue);
        } else if (keyValue.first[0] > letter) {
            break;
        }
    }

    // Sorting the key-value pairs in descending order by the number of appearances,
    // then in alphabetical order by the word itself
    std::sort(keyValues.begin(), keyValues.end(), [](const pair<string, vector<int>>& a, const pair<string, vector<int>>& b) {
        if (a.second.size() == b.second.size()) {
            return a.first < b.first;
        }
        return a.second.size() > b.second.size();
    });

    // Erase keys from the map
    for (const auto& keyValue : keyValues) {
        wordMap.erase(keyValue.first);
    }

    return keyValues;
}

void *reduceFunc(void *arg)
{
    reduceThreadArgs *args = (reduceThreadArgs *)arg;

    // Wait for all map threads to finish
    pthread_barrier_wait(args->mapReduceBarrier);

    // Lock the mutex for reading from the wordMaps for the while loop
    pthread_mutex_lock(args->readWriteWordMapsMutex);

    // If there are more than 2 maps, we should merge them
    while (args->wordMaps->size() > 1) {
        map<string, vector<int>> firstMap = args->wordMaps->back();
        args->wordMaps->pop_back();

        map<string, vector<int>> secondMap = args->wordMaps->back();
        args->wordMaps->pop_back();

        // Unlock the mutex for reading from the wordMaps
        pthread_mutex_unlock(args->readWriteWordMapsMutex);

        // Merge the 2 maps
        for (auto word : secondMap) {
            if (firstMap.find(word.first) == firstMap.end()) {
                firstMap[word.first] = word.second;
            } else {
                firstMap[word.first].insert(firstMap[word.first].end(), word.second.begin(), word.second.end());
                sort(firstMap[word.first].begin(), firstMap[word.first].end());
            }
        }

        // Lock the mutex for writing in the final map
        pthread_mutex_lock(args->readWriteWordMapsMutex);
        
        // Put the merged map in the shared resource map
        args->wordMaps->push_back(firstMap);
    }

    // Unlock the mutex for reading from the wordMaps
    pthread_mutex_unlock(args->readWriteWordMapsMutex);

    // Wait for all reduce threads to finish
    pthread_barrier_wait(args->reduceBarrier);

    // Now its time for writing in the output files
    // Lock the mutex for writing in the output files
    pthread_mutex_lock(args->writeOutputFilesMutex);

    while (!args->outputChars->empty()) {
        // Get a char from the queue
        char letter = args->outputChars->front();
        
        // Remove the char from the queue
        args->outputChars->erase(args->outputChars->begin());

        vector<pair<string, vector<int>>> keys = getKeysStartingWith(args->wordMaps->front(), letter);

        // Unlock the mutex for writing in the output files
        pthread_mutex_unlock(args->writeOutputFilesMutex);

        string outputFileName = string(1, letter) + ".txt";
        ofstream outputFile(outputFileName);
                
        for (const auto& key : keys) {
            outputFile << key.first << ":[";
            for (size_t i = 0; i < key.second.size(); ++i) {
                outputFile << key.second[i];
                if (i != key.second.size() - 1) {
                    outputFile << " ";
                }
            }
            outputFile << "]" << endl;
        }

        cout << "Thread " << args->id << " wrote in " << outputFileName << endl;
        outputFile.close();

        // Lock the mutex for checking next chars
        pthread_mutex_lock(args->writeOutputFilesMutex);
    }
    
    pthread_mutex_unlock(args->writeOutputFilesMutex);

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

        ifstream currentFile(currentFileName);

        map<string, vector<int>> wordMap = map<string, vector<int>>();
        string line;
        
        while (getline(currentFile, line)) {
            istringstream lineStream(line);
            string word;

            while (lineStream >> word) {
                string filteredWord = filterWord(word);
                // Add the current file index to the vector of the word
                // Check if the word is already in the map, the index will stay only one time
                if (!filteredWord.empty() && wordMap.find(filteredWord) == wordMap.end()) {
                    wordMap[filteredWord].push_back(currentFileIndex);
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
    pthread_mutex_t *readWriteWordMapsMutex = new pthread_mutex_t;
    pthread_mutex_init(readWriteWordMapsMutex, NULL);

    // Create the mutex for extracting from the map and writing in the output files
    pthread_mutex_t *writeOutputFilesMutex = new pthread_mutex_t;
    pthread_mutex_init(writeOutputFilesMutex, NULL);

    // Create the barrier for reduce threads
    pthread_barrier_t *reduceBarrier = new pthread_barrier_t;
    pthread_barrier_init(reduceBarrier, NULL, noOfReduceThreads);

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
    vector<map<string, vector<int>>> wordMaps = vector<map<string, vector<int>>>();

    // Create the vector with all the english alphabet characters
    vector<char> outputChars = vector<char>();
    const char alphabet[] = "abcdefghijklmnopqrstuvwxyz";
    for (char c : alphabet) {
        outputChars.push_back(c);
    }

    for (int i = 0; i < noOfMapThreads + noOfReduceThreads; i++)
    {
        if (i < noOfMapThreads) {

            mapThreadArgs *args = new mapThreadArgs();
            args->id = i;
            args->readFromInputFileMutex = readFromInputFileMutex;

            args->inputFiles = inputFiles;
            args->wordMaps = &wordMaps;
            args->writeWordMapsMutex = writeWordMapsMutex;
            args->mapReduceBarrier = mapReduceBarrier;

            int r = pthread_create(&mapThreads[i], NULL, mapFunc, args);

            if (r) {
                cout << "Error creating thread " << i << endl;
                return -1;
            }
        } else {
            reduceThreadArgs *args = new reduceThreadArgs();
            args->id = i;
            args->readWriteWordMapsMutex = readWriteWordMapsMutex;
            args->wordMaps = &wordMaps;

            map<string, vector<int>> *finalMap = new map<string, vector<int>>();

            args->mapReduceBarrier = mapReduceBarrier;
            args->writeOutputFilesMutex = writeOutputFilesMutex;

            args->outputChars = &outputChars;
            args->reduceBarrier = reduceBarrier;

            int r = pthread_create(&reduceThreads[i - noOfMapThreads], NULL, reduceFunc, args);

            if (r) {
                cout << "Error creating thread " << i << endl;
                return -1;
            }
        }
    }

    void *status;
    for (int i = 0; i < noOfMapThreads + noOfReduceThreads; i++) {
        int r = pthread_join(i < noOfMapThreads ? mapThreads[i] : reduceThreads[i - noOfMapThreads], &status);

		if (r) {
			cout << "Error joining thread " << i << endl;
            return -1;
		}
	}

    // Destroy the mutexes
    pthread_mutex_destroy(readFromInputFileMutex);
    pthread_mutex_destroy(writeWordMapsMutex);
    pthread_mutex_destroy(readWriteWordMapsMutex);
    pthread_mutex_destroy(writeOutputFilesMutex);

    // Destroy the barrier
    pthread_barrier_destroy(mapReduceBarrier);
    pthread_barrier_destroy(reduceBarrier);

    // Write all the values from wordMaps
    // for (auto wordMap : wordMaps) {
    //     for (auto word : wordMap) {
    //         cout << word.first << ": ";
    //         for (auto index : word.second) {
    //             cout << index << " ";
    //         }
    //         cout << endl;
    //     }
    // }

    return 0;
}