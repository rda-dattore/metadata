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
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <datetime.hpp>
#include <metadata.hpp>
#include <metadata_export_pg.hpp>
#include <tokendoc.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using metautils::log_error;
using metautils::log_warning;
using std::cerr;
using std::cout;
using std::deque;
using std::endl;
using std::regex;
using std::regex_search;
using std::string;
using std::stringstream;
using std::to_string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strutils::append;
using strutils::split;
using strutils::strand;
using unixutils::mysystem2;
using xercesc_3_2::XercesDOMParser;
using xercesc_3_2::ErrorHandler;
using xercesc_3_2::XMLPlatformUtils;
using xercesc_3_2::SAXParseException;
using xercesc_3_2::XMLString;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";

const string USER = getenv("USER");

struct LocalArgs {
  LocalArgs() : git_repos(),queued_only(false),all_non_public(false) {}

  vector<string> git_repos;
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

  string file_name,uflag,parse_error;
  pthread_t tid;
  bool load_alternate_schema;
};

class ParserErrorHandler : public ErrorHandler
{
public:
  ParserErrorHandler() : parse_error() {}

  void warning(const SAXParseException& e) {
    char *msg=XMLString::transcode(e.getMessage());
    if (!parse_error.empty()) {
      parse_error+="\n";
    }
    parse_error+="Warning ("+to_string(e.getLineNumber())+":"+to_string(e.getColumnNumber())+"): "+string(msg);
    XMLString::release(&msg);
  }
  void error(const SAXParseException& e) {
    char *msg=XMLString::transcode(e.getMessage());
    if (!parse_error.empty()) {
      parse_error+="\n";
    }
    parse_error+="Error ("+to_string(e.getLineNumber())+":"+to_string(e.getColumnNumber())+"): "+string(msg);
    XMLString::release(&msg);
  }
  void fatalError(const SAXParseException& e) {
    char *msg=XMLString::transcode(e.getMessage());
    if (!parse_error.empty()) {
      parse_error+="\n";
    }
    parse_error+="Fatal error ("+to_string(e.getLineNumber())+":"+to_string(e.getColumnNumber())+"): "+string(msg);
    XMLString::release(&msg);
  }
  void resetErrors() {
  }

  string parse_error;
};

extern "C" void segv_handler(int) {
  stringstream oss,ess;
  mysystem2("/bin/tcsh -c \"/gpfs/u/home/dattore/bin/vm/dset_waf DBRESET\"",
      oss, ess);
  cout << "Database was reset for the next update" << endl;
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
  static XercesDOMParser *parser=nullptr;
  static ParserErrorHandler *parserErrorHandler;
  if (parser == nullptr) {
    XMLPlatformUtils::Initialize();
    parser=new XercesDOMParser;
    parserErrorHandler=new ParserErrorHandler;
    parser->setValidationScheme(XercesDOMParser::Val_Auto);
    parser->setDoNamespaces(true);
    parser->setDoSchema(true);
    parser->setValidationConstraintFatal(true);
    parser->cacheGrammarFromParse(true);
    parser->setErrorHandler(parserErrorHandler);
  }
  else if (t->load_alternate_schema) {
    delete parser;
    parser=new XercesDOMParser;
    parserErrorHandler=new ParserErrorHandler;
    parser->setValidationScheme(XercesDOMParser::Val_Auto);
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
    t->parse_error = "";
  } else {
    t->parse_error=parserErrorHandler->parse_error;
/*
// patch to ignore timestamp failures that are not real
if (t->parse_error.find("Z' does not match any member types of the union") != string::npos) {
t->parse_error = "";
}
*/
  }
  return nullptr;
}

void do_push(const deque<string>& arg_list) {
  Server server(metautils::directives.database_server, "metadata", "metadata",
      "rdadb");

  // clean up any datasets from previously failed pushes
  LocalQuery q("dsid, uflag", "metautil.dset_waf", "uflag != ''");
  if (q.submit(server) == 0 && q.num_rows() > 0) {
    for (const auto& r : q) {
      server._delete("metautil.dset_waf", "dsid = '" + r[0] + "' and uflag = '"
          + r[1] + "'");
    }
  }
  unordered_set<string> work_in_progress_datasets, queued_datasets;
  if (local_args.queued_only) {
    q.set("dsid", "search.datasets", "type = 'W'");
    if (q.submit(server) != 0) {
      log_error("Database error while getting work-in-progress datasets: '" +
          q.error() + "'", "dset_waf", USER);
      exit(1);
    }
    for (const auto& r : q) {
      work_in_progress_datasets.emplace(r[0]);
    }
    q.set("dsid", "metautil.dset_waf", "uflag = ''");
    if (q.submit(server) != 0) {
      log_error("Database error while getting queued datasets: '" + q.error() +
          "'", "dset_waf", USER);
      exit(1);
    }
    for (const auto& r : q) {

      // ignore work-in-progress datasets
      if (work_in_progress_datasets.find(r[0]) == work_in_progress_datasets.
          end()) {
        queued_datasets.emplace(r[0]);
      }
    }

    // no datasets currently in the queue, so no work to do
    if (queued_datasets.empty()) {
      exit(0);
    }
  }
  vector<string> dsids;
  if (arg_list.front() == "all") {

    // push all datasets
    if (arg_list.size() > 1) {
      cerr << "the dataset list must be \"all\" OR a list of dataset numbers" <<
          endl;
      exit(1);
    } else {
      LocalQuery q("select dsid from search.datasets where (type = 'P' or type "
          "= 'H') and dsid < 'ds999.0'");
      if (q.submit(server) == 0) {
        for (const auto& r : q) {
          if (!local_args.queued_only || queued_datasets.find(r[0]) !=
              queued_datasets.end()) {
            dsids.emplace_back(r[0]);
          }
        }
      }
    }
  } else {

    // only push specified datasets
    for (const auto& arg : arg_list) {
      if (arg == "all") {
        cerr << "the dataset list must be \"all\" OR a list of dataset numbers"
            << endl;
        exit(1);
      }
      LocalQuery q("type", "search.datasets", "dsid = '" + arg + "'");
      if (q.submit(server) == 0) {
        Row r;
        if (q.fetch_row(r) && regex_search(r[0], regex("^[PH]$"))) {
          if (!local_args.queued_only || queued_datasets.find(arg) !=
              queued_datasets.end()) {
            dsids.emplace_back(arg);
          }
        }
      }
    }
  }
  string uflag;
  if (dsids.empty()) {
    if ((local_args.queued_only && !queued_datasets.empty()) || !local_args.
        queued_only) {
      cerr << "no matching datasets found " << local_args.queued_only << " " <<
          queued_datasets.size() << endl;
    }
    exit(1);
  }
  if (!queued_datasets.empty()) {
    uflag = strand(10);
    for (const auto& dsid : dsids) {
      if (server.update("metautil.dset_waf", "uflag = '" + uflag + "'", "dsid "
          "= '" + dsid + "'") < 0) {
        log_error("flag set failed for flag '" + uflag + "'", "dset_waf", USER);
      }
    }
  }
  static const string LOCAL_WAF = "/data/ptmp/dattore";
  for (auto& dsid : dsids) {
    unique_ptr<TokenDocument> tdoc;
    stringstream metadata_record;
    metadataExport::export_metadata("iso19139", tdoc, metadata_record, dsid, 0);
    if (metadata_record.str().empty()) {
      cerr << "Warning: empty metadata record for " << dsid << endl;
      dsid = "";
      continue;
    }
    string fname = LOCAL_WAF + "/waf-ds" + dsid + ".xml";
    std::ofstream ofs;
    ofs.open(fname.c_str());
    if (!ofs.is_open()) {
      log_error("Error opening output file '" + fname + "'; uflag was '" + uflag
          + "'", "dset_waf", USER);
    }
    ofs << "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>" << endl;
    auto date_stamp_index = metadata_record.str().find("<gmd:dateStamp>");
    auto date_time_index = metadata_record.str().find("<gco:DateTime>",
        date_stamp_index);
    ofs << metadata_record.str().substr(0, date_time_index + 14) << dateutils::
        current_date_time().to_string("%Y-%m-%dT%H:%MM:%SS") << metadata_record.
        str().substr(date_time_index + 33) << endl;
    ofs.close();
    ofs.clear();

    // validate the ISO record before pushing it to the WAF
    ValidatorThreadStruct vts;
    vts.file_name = fname;
    vts.uflag = uflag;
    vts.load_alternate_schema = false;
    pthread_create(&vts.tid, nullptr, run_validator, &vts);
    TimerThreadStruct tts;
    tts.timeout = 180;
    tts.validator_tid = vts.tid;
    pthread_create(&tts.tid, nullptr, run_timer, &tts);
    pthread_join(vts.tid, nullptr);
    pthread_cancel(tts.tid);
    if (tts.timed_out || regex_search(vts.parse_error, regex("unable to open "
        "file"))) {
      vts.load_alternate_schema = true;
      pthread_create(&vts.tid, nullptr, run_validator, &vts);
      tts.timeout = 180;
      pthread_create(&tts.tid, nullptr, run_timer, &tts);
      pthread_join(vts.tid, nullptr);
      pthread_cancel(tts.tid);
    }
    if (tts.timed_out) {
      log_error("parser timed out for flag '" + uflag + "'", "dset_waf", USER);
    } else if (!vts.parse_error.empty()) {
      log_error(dsid + " - XML validation failed; error(s) follow:\n" + vts.
          parse_error + "\nuflag was '" + uflag + "'", "dset_waf", USER);
    }
  }
  static const string REPO_HEAD = "/data/ptmp/dattore/git-repos";
  for (const auto& repo : local_args.git_repos) {
    stringstream oss, ess;
    auto exit_status = mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo +
        "; git stash; git pull -q\"", oss, ess);

    // 'git stash list' to see list of stashes
    //   may need to do a 'git stash drop "stash@{0}"'?
    if (!ess.str().empty()) {
      if (exit_status == 0) {
        log_warning("git pull message: '" + ess.str() + "'; uflag was '" +
            uflag + "'", "dset_waf", USER);
      } else {
        log_error("git pull error: '" + ess.str() + "'; uflag was '" + uflag +
            "'", "dset_waf", USER);
      }
    }
    size_t num_added = 0;
    for (const auto& dsid : dsids) {
      if (!dsid.empty()) {
        string fname = LOCAL_WAF + "/waf-ds" + dsid + ".xml";
        stringstream oss, ess;
        mysystem2("/bin/tcsh -c \"cp " + fname + " " + REPO_HEAD + "/" + repo +
            "/ds" + dsid + ".xml\"", oss, ess);
        mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo + "; git add ds"
            + dsid + ".xml\"", oss, ess);
        if (!ess.str().empty()) {
          log_error("git add error for " + dsid + ": '" + ess.str() + "'; "
              "uflag was '" + uflag + "'", "dset_waf", USER);
        } else {
          ++num_added;
        }
      }
    }
    if (num_added > 0) {
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo + "; git commit "
          "-m 'auto update' -a\"", oss, ess);
      if (!ess.str().empty()) {
        log_error("git commit error: '" + ess.str() + "'; uflag was '" + uflag +
            "'", "dset_waf", USER);
      } else {
        mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo + "; git push "
            "-q\"", oss, ess);
        if (!ess.str().empty() && ess.str().find("remote: Resolving deltas") != 0) {
          log_error("git push error: '" + ess.str() + "'; uflag was '" + uflag +
              "'", "dset_waf", USER);
        }
      }
    }
  }
  if (!uflag.empty()) {
    if (server._delete("metautil.dset_waf", "uflag = '" + uflag + "'") < 0) {
      log_error("unable to remove queued datasets with flag '" + uflag + "'",
          "dset_waf", USER);
    }
  }
  server.disconnect();
  for (const auto& repo : local_args.git_repos) {
    stringstream oss, ess;
    while (ess.str().empty()) {
      mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo + "; git stash "
          "drop 'stash@{0}'\"", oss, ess);
    }
  }
}

void do_delete(const deque<string>& arg_list) {
  vector<string> dsids;
  if (local_args.all_non_public) {
    Server server(metautils::directives.database_server, "metadata", "metadata",
        "rdadb");
    LocalQuery query("dsid", "search.datasets", "type not in ('P', 'H')");
    if (query.submit(server) < 0) {
      log_error("Database error while getting non-public datasets: '" + query.
          error() + "'", "dset_waf", USER);
      exit(1);
    }
    for (const auto& row : query) {
      dsids.emplace_back(row[0]);
    }
    server.disconnect();
  } else {
    if (arg_list.front() == "all") {
      cerr << "Error: datasets can only be deleted by explicit naming" << endl;
      exit(1);
    } else {
      for (const auto& arg : arg_list) {
        if (arg != "all") {
          dsids.emplace_back(arg);
        }
      }
    }
  }
  static const string REPO_HEAD = "/data/ptmp/dattore/git-repos";
  for (const auto& repo : local_args.git_repos) {
    string repo_tail;
    auto idx = repo.find("/");
    if (idx != string::npos) {
      repo_tail = repo.substr(idx+1) + "/";
    }
    size_t num_deleted = 0;
    for (const auto& dsid : dsids) {
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo + "; git rm ds" +
          dsid + ".xml\"", oss, ess);
      if (!ess.str().empty()) {
        if (ess.str() != "fatal: pathspec 'ds"+dsid+".xml' did not match any "
            "files\n") {
          log_warning("git rm error for " + dsid + ": '" + ess.str() + "'",
              "dset_waf", USER);
        }
      } else {
        ++num_deleted;
      }
    }
    if (num_deleted > 0) {
      stringstream oss, ess;
      mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo + "; git pull "
          "-q\"", oss, ess);
      if (!ess.str().empty()) {
        log_error("git pull error: '" + ess.str() + "'", "dset_waf", USER);
      } else {
        mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo + "; git "
            "commit -m 'auto remove' -a\"", oss, ess);
        if (!ess.str().empty()) {
          log_error("git commit error: '" + ess.str() + "'", "dset_waf", USER);
        } else {
          mysystem2("/bin/tcsh -c \"cd " + REPO_HEAD + "/" + repo + "; git "
              "push -q\"", oss, ess);
          if (!ess.str().empty()) {
            log_error("git push error: '" + ess.str() + "'", "dset_waf", USER);
          }
        }
      }
    }
  }
}

void do_db_reset(const deque<string>& arg_list) {
  Server server(metautils::directives.database_server, "metadata", "metadata",
      "rdadb");
  LocalQuery query("distinct dsid", "metautil.dset_waf");
  if (query.submit(server) < 0) {
    log_error("Database error while trying to fix a failed push: '" + query.
        error() + "'", "dset_waf", USER);
    exit(1);
  }
  if (server._delete("metautil.dset_waf") < 0) {
    log_error("Database error while trying to clear a failed push: '" + server.
        error() + "'", "dset_waf", USER);
    exit(1);
  }
  auto status = 0;
  string dslist;
  for (const auto& row : query) {
    append(dslist, row[0], ", ");
    if (server.insert("metautil.dset_waf", "'" + row[0] + "', ''") < 0) {
      status = -1;
    }
  }
  if (status < 0) {
    log_error("Database error while trying to clear a failed push - dataset "
        "list: '" + dslist + "'", "dset_waf", USER);
    exit(1);
  }
}

int main(int argc, char **argv) {
  if (argc < 3 && (argc != 2 || string(argv[1]) != "DBRESET")) {
    cerr << "usage: dset_waf <action> [options...] <dsid_list>" << endl;
    cerr << "\nrequired:" << endl;
    cerr << "<action>  must be one of:" << endl;
    cerr << "            PUSH - add/update dataset(s)" << endl;
    cerr << "            DELETE - remove dataset(s)" << endl;
    cerr << "            DBRESET - reset the database after a failed push" << endl;
    cerr << "<dslist>  must be one of:" << endl;
    cerr << "            \"all\" for all datasets" << endl;
    cerr << "            \"dnnnnnn ...\" one or more individual dataset numbers" << endl;
    cerr << "\noptions:" << endl;
    cerr << "-R <repo>  restrict to <repo>, otherwise by default, operate on all known repos" << endl;
    cerr << "\nPUSH options:" << endl;
    cerr << "--queued-only  only push datasets that are queued in the database" << endl;
    cerr << "\nDELETE options:" << endl;
    cerr << "--all-non-public  identify and delete all non-public datasets" << endl;
    exit(1);
  }
  metautils::args.args_string = unixutils::unix_args_string(argc, argv, '%');
  auto arg_list = split(metautils::args.args_string, "%");
  auto action = arg_list.front();
  arg_list.pop_front();
  while (!arg_list.empty()) {
    if (arg_list.front()[0] == '-') {
      if (arg_list.front()[1] == 'R') {
        arg_list.pop_front();
        local_args.git_repos.emplace_back(arg_list.front());
      } else if (arg_list.front() == "--queued-only") {
        local_args.queued_only = true;
      } else if (arg_list.front() == "--all-non-public") {
        local_args.all_non_public = true;
      }
      arg_list.pop_front();
    } else {
      break;
    }
  }
  if (action != "DBRESET" && arg_list.empty()) {
    cerr << "Error: no dataset(s) specified" << endl;
    exit(1);
  }
  if (local_args.git_repos.empty()) {
    local_args.git_repos.emplace_back("dset-web-accessible-folder-dev/rda");
    local_args.git_repos.emplace_back("dash-rda-prod/RDA-Datasets");
  }
//  signal(SIGSEGV,segv_handler);
  metautils::read_config("dset_waf", "", "");
  if (action == "PUSH") {
    do_push(arg_list);
  } else if (action == "DELETE") {
    do_delete(arg_list);
  } else if (action == "DBRESET") {
    do_db_reset(arg_list);
  } else {
    cerr << "Error: invalid action" << endl;
    exit(1);
  }
}
