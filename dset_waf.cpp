#include <iostream>
#include <sstream>
#include <regex>
#include <unordered_set>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/parsers/XercesDOMParser.hpp>
#include <xercesc/sax/ErrorHandler.hpp>
#include <xercesc/sax/SAXParseException.hpp>
#include <xercesc/validators/common/Grammar.hpp>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <strutils.hpp>
#include <utils.hpp>
#include <datetime.hpp>
#include <metadata.hpp>
#include <metadata_export.hpp>
#include <tokendoc.hpp>
#include <myerror.hpp>

metautils::Directives meta_directives;
metautils::Args meta_args;
std::string myerror="";
std::string mywarning="";

std::string user=getenv("USER");

struct LocalArgs {
  LocalArgs() : git_repos(),queued_only(false),all_non_public(false) {}

  std::vector<std::string> git_repos;
  bool queued_only,all_non_public;
} local_args;
struct TimerThreadStruct {
  TimerThreadStruct() : timeout(0),tid(0),validator_tid(),timed_out() {}

  size_t timeout;
  pthread_t tid,validator_tid;
  bool timed_out;
};
struct ValidatorThreadStruct {
  ValidatorThreadStruct() : file_name(),uflag(),parse_error(),tid(0),load_alternate_schema(false) {}

  std::string file_name,uflag,parse_error;
  pthread_t tid;
  bool load_alternate_schema;
};

class ParserErrorHandler : public xercesc_3_1::ErrorHandler
{
public:
  ParserErrorHandler() : parse_error() {}

  void warning(const xercesc_3_1::SAXParseException& e) {
    char *msg=xercesc_3_1::XMLString::transcode(e.getMessage());
    if (!parse_error.empty()) {
	parse_error+="\n";
    }
    parse_error+="Warning ("+strutils::itos(e.getLineNumber())+":"+strutils::itos(e.getColumnNumber())+"): "+std::string(msg);
    xercesc_3_1::XMLString::release(&msg);
  }
  void error(const xercesc_3_1::SAXParseException& e) {
    char *msg=xercesc_3_1::XMLString::transcode(e.getMessage());
    if (!parse_error.empty()) {
	parse_error+="\n";
    }
    parse_error+="Error ("+strutils::itos(e.getLineNumber())+":"+strutils::itos(e.getColumnNumber())+"): "+std::string(msg);
    xercesc_3_1::XMLString::release(&msg);
  }
  void fatalError(const xercesc_3_1::SAXParseException& e) {
    char *msg=xercesc_3_1::XMLString::transcode(e.getMessage());
    if (!parse_error.empty()) {
	parse_error+="\n";
    }
    parse_error+="Fatal error ("+strutils::itos(e.getLineNumber())+":"+strutils::itos(e.getColumnNumber())+"): "+std::string(msg);
    xercesc_3_1::XMLString::release(&msg);
  }
  void resetErrors() {
  }

  std::string parse_error;
};

extern "C" void segv_handler(int)
{
  std::stringstream oss,ess;
  unixutils::mysystem2("/bin/tcsh -c \"/gpfs/u/home/dattore/bin/vm/dset_waf DBRESET\"",oss,ess);
  std::cout << "Database was reset for the next update" << std::endl;
}


extern "C" void *run_timer(void *tts)
{
  TimerThreadStruct *t=(TimerThreadStruct *)tts;
  t->timed_out=false;
  sleep(t->timeout);
  t->timed_out=true;
  pthread_cancel(t->validator_tid);
  return nullptr;
}

extern "C" void *run_validator(void *vts)
{
  ValidatorThreadStruct *t=(ValidatorThreadStruct *)vts;
  static xercesc_3_1::XercesDOMParser *parser=nullptr;
  static ParserErrorHandler *parserErrorHandler;
  if (parser == nullptr) {
    xercesc_3_1::XMLPlatformUtils::Initialize();
    parser=new xercesc_3_1::XercesDOMParser;
    parserErrorHandler=new ParserErrorHandler;
    parser->setValidationScheme(xercesc_3_1::XercesDOMParser::Val_Auto);
    parser->setDoNamespaces(true);
    parser->setDoSchema(true);
    parser->setValidationConstraintFatal(true);
    parser->cacheGrammarFromParse(true);
    parser->setErrorHandler(parserErrorHandler);
  }
  else if (t->load_alternate_schema) {
    delete parser;
    parser=new xercesc_3_1::XercesDOMParser;
    parserErrorHandler=new ParserErrorHandler;
    parser->setValidationScheme(xercesc_3_1::XercesDOMParser::Val_Auto);
    parser->setDoNamespaces(true);
    parser->setDoSchema(true);
    parser->setValidationConstraintFatal(true);
    parser->cacheGrammarFromParse(true);
    parser->setErrorHandler(parserErrorHandler);
parser->setExternalSchemaLocation("http://www.isotc211.org/2005/gmd /usr/local/www/server_root/web/metadata/schemas/iso/gmd/gmd.xsd");
  }
  parserErrorHandler->parse_error="";
  parser->parse(t->file_name.c_str());
  if (parserErrorHandler->parse_error.empty()) {
    t->parse_error="";
  }
  else {
    t->parse_error=parserErrorHandler->parse_error;
  }
  return nullptr;
}

void do_push(const std::deque<std::string>& arg_list)
{
  MySQL::Server server(meta_directives.database_server,"metadata","metadata","");
  std::unordered_set<std::string> work_in_progress_datasets,queued_datasets;
  if (local_args.queued_only) {
    MySQL::LocalQuery query("dsid","search.datasets","type = 'W'");
    if (query.submit(server) != 0) {
	metautils::log_error("Database error while getting work-in-progress datasets: '"+query.error()+"'","dset_waf",user);
	exit(1);
    }
    MySQL::Row row;
    while (query.fetch_row(row)) {
	work_in_progress_datasets.emplace(row[0]);
    }
    query.set("dsid","metautil.dset_waf","uflag = ''");
    if (query.submit(server) != 0) {
	metautils::log_error("Database error while getting queued datasets: '"+query.error()+"'","dset_waf",user);
	exit(1);
    }
    while (query.fetch_row(row)) {
	if (work_in_progress_datasets.find(row[0]) == work_in_progress_datasets.end()) {
// ignore work-in-progress datasets
	  queued_datasets.emplace(row[0]);
	}
    }
    if (queued_datasets.size() == 0) {
// no datasets currently in the queue, so no work to do
	exit(0);
    }
  }
  std::vector<std::string> dsids;
  if (arg_list.front() == "all") {
    if (arg_list.size() > 1) {
	std::cerr << "the dataset list must be \"all\" OR a list of dataset numbers" << std::endl;
	exit(1);
    }
    else {
	MySQL::LocalQuery query("select dsid from search.datasets where (type = 'P' or type = 'H') and dsid != 'ds999.9'");
	if (query.submit(server) == 0) {
	  MySQL::Row row;
	  while (query.fetch_row(row)) {
	    if (queued_datasets.size() == 0 || queued_datasets.find(row[0]) != queued_datasets.end()) {
		dsids.emplace_back(row[0]);
	    }
	  }
	}
    }
  }
  else {
    for (const auto& arg : arg_list) {
	if (arg == "all") {
	  std::cerr << "the dataset list must be \"all\" OR a list of dataset numbers" << std::endl;
	  exit(1);
	}
	MySQL::LocalQuery query("type","search.datasets","dsid = '"+arg+"'");
	if (query.submit(server) == 0) {
	  MySQL::Row row;
	  if (query.fetch_row(row) && std::regex_search(row[0],std::regex("^[PH]$"))) {
	    if (queued_datasets.size() == 0 || queued_datasets.find(arg) != queued_datasets.end()) {
		dsids.emplace_back(arg);
	    }
	  }
	}
    }
  }
  std::string uflag;
  if (dsids.size() == 0) {
    if ((local_args.queued_only && queued_datasets.size() > 0) || !local_args.queued_only) {
	std::cerr << "no matching datasets found " << local_args.queued_only << " " << queued_datasets.size() << std::endl;
    }
    exit(1);
  }
  else if (queued_datasets.size() > 0) {
    uflag=strutils::strand(10);
    for (const auto& dsid : dsids) {
	if (server.update("metautil.dset_waf","uflag = '"+uflag+"'","dsid = '"+dsid+"'") < 0) {
	  metautils::log_error("flag set failed for flag '"+uflag+"'","dset_waf",user);
	}
    }
  }
  static const std::string LOCAL_WAF="/gpfs/fs1/scratch/dattore";
  for (const auto& dsid : dsids) {
    std::string fname=LOCAL_WAF+"/waf-ds"+dsid+".xml";
    std::ofstream ofs;
    ofs.open(fname.c_str());
    if (!ofs.is_open()) {
	metautils::log_error("Error opening output file '"+fname+"'; uflag was '"+uflag+"'","dset_waf",user);
    }
    else {
	std::unique_ptr<TokenDocument> tdoc;
	ofs << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << std::endl;
	std::stringstream metadata_record;
	metadataExport::export_metadata("iso19139",tdoc,metadata_record,dsid,0);
	auto date_stamp_index=metadata_record.str().find("<gmd:dateStamp>");
	auto date_time_index=metadata_record.str().find("<gco:DateTime>",date_stamp_index);
	ofs << metadata_record.str().substr(0,date_time_index+14) << dateutils::current_date_time().to_string("%Y-%m-%dT%H:%MM:%SS") << metadata_record.str().substr(date_time_index+33) << std::endl;
	ofs.close();
	ofs.clear();
	ValidatorThreadStruct vts;
	vts.file_name=fname;
	vts.uflag=uflag;
	vts.load_alternate_schema=false;
	pthread_create(&vts.tid,NULL,run_validator,&vts);
	TimerThreadStruct tts;
	tts.timeout=180;
	tts.validator_tid=vts.tid;
	pthread_create(&tts.tid,NULL,run_timer,&tts);
	pthread_join(vts.tid,NULL);
	pthread_cancel(tts.tid);
	if (tts.timed_out) {
	  vts.load_alternate_schema=true;
	  pthread_create(&vts.tid,NULL,run_validator,&vts);
	  tts.timeout=180;
	  pthread_create(&tts.tid,NULL,run_timer,&tts);
	  pthread_join(vts.tid,NULL);
	  pthread_cancel(tts.tid);
	}
	if (tts.timed_out) {
	  metautils::log_error("parser timed out for flag '"+uflag+"'","dset_waf",user);
	}
	else if (!vts.parse_error.empty()) {
	  metautils::log_error(dsid+" - XML validation failed; error(s) follow:\n"+vts.parse_error+"\nuflag was '"+uflag+"'","dset_waf",user);
	}
    }
  }
  static const std::string REPO_HEAD="/data/ptmp/dattore/git-repos";
  for (const auto& repo : local_args.git_repos) {
    std::stringstream oss,ess;
    unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git stash; git pull -q\"",oss,ess);
// 'git stash list' to see list of stashes
// may need to do a 'git stash drop "stash@{0}"'?
    if (!ess.str().empty()) {
	metautils::log_error("git pull error: '"+ess.str()+"'; uflag was '"+uflag+"'","dset_waf",user);
    }
    size_t num_added=0;
    for (const auto& dsid : dsids) {
	std::string fname=LOCAL_WAF+"/waf-ds"+dsid+".xml";
	std::stringstream oss,ess;
	unixutils::mysystem2("/bin/tcsh -c \"cp "+fname+" "+REPO_HEAD+"/"+repo+"/ds"+dsid+".xml\"",oss,ess);
	unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git add ds"+dsid+".xml\"",oss,ess);
	if (!ess.str().empty()) {
	  metautils::log_error("git add error for "+dsid+": '"+ess.str()+"'; uflag was '"+uflag+"'","dset_waf",user);
	}
	else {
	  ++num_added;
	}
    }
    if (num_added > 0) {
	std::stringstream oss,ess;
	unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git commit -m 'auto update' -a\"",oss,ess);
	if (!ess.str().empty()) {
	  metautils::log_error("git commit error: '"+ess.str()+"'; uflag was '"+uflag+"'","dset_waf",user);
	}
	else {
	  unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git push -q\"",oss,ess);
	  if (!ess.str().empty()) {
	    metautils::log_error("git push error: '"+ess.str()+"'; uflag was '"+uflag+"'","dset_waf",user);
	  }
	}
    }
  }
  if (!uflag.empty()) {
    if (server._delete("metautil.dset_waf","uflag = '"+uflag+"'") < 0) {
	metautils::log_error("unable to remove queued datasets with flag '"+uflag+"'","dset_waf",user);
    }
  }
  server.disconnect();
  for (const auto& repo : local_args.git_repos) {
    std::stringstream oss,ess;
    while (ess.str().empty()) {
	unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git stash drop 'stash@{0}'\"",oss,ess);
    }
  }
}

void do_delete(const std::deque<std::string>& arg_list)
{
  std::vector<std::string> dsids;
  if (local_args.all_non_public) {
    MySQL::Server server(meta_directives.database_server,"metadata","metadata","");
    MySQL::LocalQuery query("dsid","search.datasets","!find_in_set(type,'P,H')");
    if (query.submit(server) < 0) {
	metautils::log_error("Database error while getting non-public datasets: '"+query.error()+"'","dset_waf",user);
	exit(1);
    }
    MySQL::Row row;
    while (query.fetch_row(row)) {
	dsids.emplace_back(row[0]);
    }
    server.disconnect();
  }
  else {
    if (arg_list.front() == "all") {
	std::cerr << "Error: datasets can only be deleted by explicit naming" << std::endl;
	exit(1);
    }
    else {
	for (const auto& arg : arg_list) {
	  if (arg != "all") {
	    dsids.emplace_back(arg);
	  }
	}
    }
  }
  static const std::string REPO_HEAD="/data/ptmp/dattore/git-repos";
  for (const auto& repo : local_args.git_repos) {
    std::string repo_tail;
    auto idx=repo.find("/");
    if (idx != std::string::npos) {
	repo_tail=repo.substr(idx+1)+"/";
    }
    size_t num_deleted=0;
    for (const auto& dsid : dsids) {
	std::stringstream oss,ess;
	unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git rm ds"+dsid+".xml\"",oss,ess);
	if (!ess.str().empty()) {
	  if (ess.str() != "fatal: pathspec '"+repo_tail+"ds"+dsid+".xml' did not match any files\n") {
	    metautils::log_warning("git rm error for "+dsid+": '"+ess.str()+"'","dset_waf",user);
	  }
	}
	else {
	  ++num_deleted;
	}
    }
    if (num_deleted > 0) {
	std::stringstream oss,ess;
	unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git pull -q\"",oss,ess);
	if (!ess.str().empty()) {
	  metautils::log_error("git pull error: '"+ess.str()+"'","dset_waf",user);
	}
	else {
	  unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git commit -m 'auto remove' -a\"",oss,ess);
	  if (!ess.str().empty()) {
	    metautils::log_error("git commit error: '"+ess.str()+"'","dset_waf",user);
	  }
	  else {
	    unixutils::mysystem2("/bin/tcsh -c \"cd "+REPO_HEAD+"/"+repo+"; git push -q\"",oss,ess);
	    if (!ess.str().empty()) {
		metautils::log_error("git push error: '"+ess.str()+"'","dset_waf",user);
	    }
	  }
	}
    }
  }
}

void do_db_reset(const std::deque<std::string>& arg_list)
{
  MySQL::Server server(meta_directives.database_server,"metadata","metadata","");
  MySQL::LocalQuery query("distinct dsid","metautil.dset_waf");
  if (query.submit(server) < 0) {
    metautils::log_error("Database error while trying to fix a failed push: '"+query.error()+"'","dset_waf",user);
    exit(1);
  }
  if (server._delete("metautil.dset_waf") < 0) {
    metautils::log_error("Database error while trying to clear a failed push: '"+server.error()+"'","dset_waf",user);
    exit(1);
  }
  MySQL::Row row;
  auto status=0;
  std::stringstream dslist;
  while (query.fetch_row(row)) {
    if (!dslist.str().empty()) {
	dslist << ", ";
    }
    dslist << row[0];
    if (server.insert("metautil.dset_waf","'"+row[0]+"',''") < 0) {
	status=-1;
    }
  }
  if (status < 0) {
    metautils::log_error("Database error while trying to clear a failed push - dataset list: '"+dslist.str()+"'","dset_waf",user);
    exit(1);
  }
}

int main(int argc,char **argv)
{
  if (argc < 3 && (argc != 2 || std::string(argv[1]) != "DBRESET")) {
    std::cerr << "usage: dset_waf <action> [options...] <dslist>" << std::endl;
    std::cerr << "\nrequired:" << std::endl;
    std::cerr << "<action>  must be one of:" << std::endl;
    std::cerr << "            PUSH - add/update dataset(s)" << std::endl;
    std::cerr << "            DELETE - remove dataset(s)" << std::endl;
    std::cerr << "            DBRESET - reset the database after a failed push" << std::endl;
    std::cerr << "<dslist>  must be one of:" << std::endl;
    std::cerr << "            \"all\" for all datasets" << std::endl;
    std::cerr << "            \"nnn.n ...\" one or more individual dataset numbers" << std::endl;
    std::cerr << "\noptions:" << std::endl;
    std::cerr << "-R <repo>  restrict to <repo>, otherwise by default, operate on all known repos" << std::endl;
    std::cerr << "\nPUSH options:" << std::endl;
    std::cerr << "--queued-only  only push datasets that are queued in the database" << std::endl;
    std::cerr << "\nDELETE options:" << std::endl;
    std::cerr << "--all-non-public  identify and delete all non-public datasets" << std::endl;
    exit(1);
  }
  meta_args.args_string=unixutils::unix_args_string(argc,argv,'%');
  auto arg_list=strutils::split(meta_args.args_string,"%");
  auto action=arg_list.front();
  arg_list.pop_front();
  while (arg_list.size() > 0) {
    if (arg_list.front()[0] == '-') {
	if (arg_list.front()[1] == 'R') {
	  arg_list.pop_front();
	  local_args.git_repos.emplace_back(arg_list.front());
	}
	else if (arg_list.front() == "--queued-only") {
	  local_args.queued_only=true;
	}
	else if (arg_list.front() == "--all-non-public") {
	  local_args.all_non_public=true;
	}
	arg_list.pop_front();
    }
    else {
	break;
    }
  }
  if (action != "DBRESET" && arg_list.size() == 0) {
    std::cerr << "Error: no dataset(s) specified" << std::endl;
    exit(1);
  }
  if (local_args.git_repos.empty()) {
    local_args.git_repos.emplace_back("dset-web-accessible-folder-dev/rda");
    local_args.git_repos.emplace_back("dash-rda-prod");
  }
//  signal(SIGSEGV,segv_handler);
  metautils::read_config("dset_waf","","");
  if (action == "PUSH") {
    do_push(arg_list);
  }
  else if (action == "DELETE") {
    do_delete(arg_list);
  }
  else if (action == "DBRESET") {
    do_db_reset(arg_list);
  }
  else {
    std::cerr << "Error: invalid action" << std::endl;
    exit(1);
  }
}
