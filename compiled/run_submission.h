#ifndef RUN_SUBMISSION_H
#define RUN_SUBMISSION_H

#include "Options.h"
#include "xalt_types.h"

void buildEnvT(Options& options, char* env[], Table& envT);
void buildUserT(Options& options, Table& envT, Table& userT, DTable& userDT);
void extractXALTRecord(std::string& exec, Table& recordT);
void parseLDD(std::string& exec, std::vector<Libpair>& lddA);
void run_direct2db(const char* confFn, std::string& usr_cmdline, std::string& hash_id, 
                   Table& rmapT, Table& envT, Table& userT,
                   Table& recordT, std::vector<Libpair>& lddA);
void translate(Table& envT, Table& userT, DTable& userDT);

#endif //RUN_SUBMISSION_H
