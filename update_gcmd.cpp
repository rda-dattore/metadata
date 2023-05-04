#include <iostream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <regex>
#include <utils.hpp>
#include <strutils.hpp>
#include <xml.hpp>
#include <MySQL.hpp>
#include <datetime.hpp>

using std::cerr;
using std::cout;
using std::endl;
using std::make_tuple;
using std::regex;
using std::regex_search;
using std::sort;
using std::string;
using std::stringstream;
using std::to_string;
using std::tuple;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::occurs;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using strutils::trim;
using unixutils::mysystem2;

/*
**  cflag in search.GCMD_<concept_scheme>:
**    0 = unchanged
**    1 = added
**    2 = changed
**    9 = ignored by RDA
*/

const string USERNAME = "dattore";
const string PASSWORD = "8F5uXZ6b";
const string KMS_API_URL = "https://gcmd.earthdata.nasa.gov/kms";

struct Args {
  Args() : server("rda-db.ucar.edu", "metadata", "metadata", ""),
      concept_scheme(), alias(), delete_flag(strutils::strand(3)),
      concept_schemes(), rda_keywords(), force_update(false), no_gcmd_update(
      false) {
    if (!server) {
      cerr << "Error: unable to connect to metadata database" << endl;
      exit(1);
    }

/*
**  concept_schemes tuple:
**    path start field in csv file
**    path end field in csv file
**    minimum number of fields required for a valid path
**    true for only isLeaf=true nodes in
**    /concept/concept_schemes/<concept_scheme> xml file, otherwise false
*/
    concept_schemes.emplace("instruments", make_tuple(4, 5, 1, true));
    concept_schemes.emplace("locations", make_tuple(0, 4, 2, false));
    concept_schemes.emplace("platforms", make_tuple(3, 4, 1, true));
    concept_schemes.emplace("projects", make_tuple(1, 2, 1, true));
    concept_schemes.emplace("providers", make_tuple(4, 5, 1, true));
    concept_schemes.emplace("sciencekeywords", make_tuple(0, 5, 4, false));
  }

  MySQL::Server server;
  string concept_scheme, alias, delete_flag;
  unordered_map<string, tuple<size_t, size_t, size_t, bool>> concept_schemes;

/*
**  rda_keywords:
**    short indicates: add keyword (1) or delete keyword (0)
*/
  unordered_map<string, short> rda_keywords;
  bool force_update, no_gcmd_update;
} args;

string sql_ready(const string& s) {
  return substitute(s, "'", "\\'");
}

void parse_args(int argc, char **argv) {
  if (argc < 2) {
    cerr << "usage: update_gcmd [options] <concept_scheme>" << endl;
    cerr << "\noptions:" << endl;
    cerr << "--force-update       force an update even if last-update dates do "
        "not require it" << endl;
    cerr << "--add-rda <keyword>  add an RDA-specific keyword (repeatable)" <<
        endl;
    cerr << "--no-gcmd            don't update from GCMD" << endl;
    cerr << endl;
    cerr << "-A <alias>           get the concepts for <concept_scheme> from "
        "<alias> instead;" << endl;
    cerr << "                     this helps when GCMD's database is corrupted "
        "and the API is" << endl;
    cerr << "                     serving the wrong keywords for a concept "
        "scheme" << endl;
    cerr << "\nvalid concept_schemes:" << endl;
    cerr << "  instruments" << endl;
    cerr << "  locations" << endl;
    cerr << "  platforms" << endl;
    cerr << "  projects" << endl;
    cerr << "  providers" << endl;
    cerr << "  sciencekeywords" << endl;
    exit(1);
  }
  auto arglist = split(unixutils::unix_args_string(argc, argv), ":");
  auto next = arglist.begin();
  auto end = arglist.end();
  --end;
  for (; next != end; ++next) {
    if (next == arglist.end()) {
      cerr << "Error: missing concept_scheme" << endl;
      exit(1);
    }
    if (*next == "--force-update") {
      args.force_update = true;
    } else if (*next == "--add-rda") {
      ++next;
      args.rda_keywords.emplace(*next, 1);
    } else if (*next == "--no-gcmd") {
      args.no_gcmd_update = true;
    } else if (*next == "-A") {
      ++next;
      args.alias = *next;
    }
  }
  args.concept_scheme = arglist.back();
  if (args.concept_schemes.find(args.concept_scheme) == args.concept_schemes.
      end() && args.concept_scheme != "all") {
    cerr << "Error: '" << args.concept_scheme << "' is not a valid "
        "concept_scheme" << endl;
    exit(1);
  }
}

bool passed_date_check() {
  stringstream oss, ess;
  mysystem2("/bin/sh -c 'curl -k -s -S -o - \"" + KMS_API_URL +
      "/concept_schemes\"'", oss, ess);
  if (!ess.str().empty()) {
    cerr << "Error getting GCMD updateDate (1):" << endl;
    cerr << ess.str() << endl;
    exit(1);
  }
  auto index = oss.str().find("<schemes");
  if (index == string::npos) {
    cerr << "Error getting GCMD updateDate (2):" << endl;
    cerr << oss.str() << endl;
    exit(1);
  }
  XMLSnippet update(oss.str().substr(oss.str().find("<schemes")));
  if (update) {
    auto gcmd_update_date = update.element("schemes/scheme@name=" +
        args.concept_scheme).attribute_value("updateDate");

// patch until all table names have been changed
auto tbl = "GCMD_" + args.concept_scheme;
if (!MySQL::table_exists(args.server, "search." + tbl)) {
tbl = "gcmd_" + args.concept_scheme;
}
    MySQL::LocalQuery q("select update_time from information_schema.tables "
//        "where table_schema = 'search' and table_name = 'GCMD_" + args.
"where table_schema = 'search' and table_name = '" + tbl + "'");
//        concept_scheme + "'");
    if (q.submit(args.server) == 0 && q.num_rows() == 1) {
      MySQL::Row row;
      q.fetch_row(row);
      auto update_time = row[0];
      if (update_time.empty()) {
        update_time = "0001-01-01";
      }
      auto uparts = split(update_time);
      cout << "GCMD '" << args.concept_scheme << "' update date: " <<
          gcmd_update_date << endl;
      cout << "RDA database '" << args.concept_scheme << "' update date: " <<
          uparts[0] << endl;
      if (gcmd_update_date < uparts[0]) {
        cout << "No update needed for '" << args.concept_scheme << "' at this "
            "time" << endl;
        if (!args.force_update) {
          return false;
        }
        cout << "  however, proceeding with update since --force-update was "
            "specified" << endl;
      }
    } else {
      cerr << "Error getting GCMD_" << args.concept_scheme << " update date: "
          << q.error() << endl;
      exit(1);
    }
  } else {
    cerr << "Error parsing updates: " << update.parse_error() << endl;
    exit(1);
  }
  return true;
}

void update_keywords() {
  cout << "Beginning update of '" << args.concept_scheme << "' at " <<
      dateutils::current_date_time().to_string() << "..." << endl;
  stringstream oss, ess;
  mysystem2("/bin/sh -c 'curl -k -s -S -o - \"" + KMS_API_URL +
      "/concepts/concept_scheme/" + args.concept_scheme + "/?format=csv\"'",
      oss, ess);
  if (!ess.str().empty()) {
    cerr << "Error getting " << args.concept_scheme << ".csv" << endl;
    exit(1);
  }
  string version, revision_date;
  unordered_map<string, tuple<string, string, string>> csv_map;
  unordered_set<string> csv_ignore_set;
  auto csv_lines = split(oss.str(), "\n");
  if (csv_lines.front().find("\"Keyword Version:") != 0) {
    cerr << "Error: invalid csv file format" << endl;
    exit(1);
  } else {
    auto parts = split(csv_lines.front(), "\",\"");
    replace_all(parts[0], "\"Keyword Version:", "");
    trim(parts[0]);
    version = parts[0];
    if (parts[1].find("Revision:") != 0) {
      cerr << "Error: missing or invalid revision date" << endl;
      exit(1);
    }
    replace_all(parts[1], "Revision:", "");
    trim(parts[1]);
    auto rparts = split(parts[1]);
    revision_date = rparts.front();
    csv_lines.pop_front();
  }
  vector<tuple<string, size_t, regex, string>> search_re_list;
  if (args.concept_scheme == "platforms") {
    MySQL::LocalQuery query("search_regexp, obml_platform_type", "search."
        "gcmd_platforms_to_rda");
    if (query.submit(args.server) == 0) {
      for (const auto& row : query) {
        search_re_list.emplace_back(make_tuple(row[0], occurs(row[0], ", ") + 1,
            regex(row[0]), row[1]));
      }
      sort(search_re_list.begin(), search_re_list.end(),
      [](tuple<string, size_t, regex, string>& left, tuple<string, size_t,
          regex, string>& right) -> bool {
        auto nleft = std::get<1>(left);
        auto nright = std::get<1>(right);
        if (nleft > nright) {
          return true;
        } else if (nleft < nright) {
          return false;
        } else {
          regex double_comma(",,");
          if (regex_search(std::get<0>(left), double_comma)) {
            if (regex_search(std::get<0>(right), double_comma)) {
              return (std::get<0>(left) <= std::get<0>(right));
            }
            return false;
          } else {
            if (regex_search(std::get<0>(right), double_comma)) {
              return true;
            }
            return (std::get<0>(left) <= std::get<0>(right));
          }
        }
      });
    } else {
      cerr << "Error getting search_re data" << endl;
      exit(1);
    }
  }
  auto fields = split(csv_lines.front(), ",");
  auto check_len = fields.size();
  csv_lines.pop_front();
  auto path_data = args.concept_schemes[args.concept_scheme];
  for (auto& line : csv_lines) {
    if (!line.empty()) {

      // remove the trailing '"'
      line.pop_back();

      // ignore the leading '"'
      auto lparts = split(line.substr(1), "\",\"");
      if (lparts.size() != check_len) {
        cerr << "SKIPPING BAD CSV LINE: " << line << endl;
      } else {

        // lparts.back() is the UUID
        if (csv_map.find(lparts.back()) == csv_map.end()) {
          if (!lparts[std::get<0>(path_data)].empty()) {
            size_t last = std::get<0>(path_data);
            string path;
            if (lparts[std::get<0>(path_data)][0] == '"') {
              path = lparts[std::get<0>(path_data)].substr(1);
            } else {
              path = lparts[std::get<0>(path_data)];
            }
            size_t num_in_path = 1;
            for (size_t n = std::get<0>(path_data) + 1; n <= std::get<1>(
                path_data); ++n) {
              if (!lparts[n].empty()) {
                path += " > " + lparts[n];
                last = n;
                ++num_in_path;
              }
            }
            if (num_in_path >= std::get<2>(path_data)) {
              if (args.concept_scheme == "sciencekeywords" && path.find(
                  "EARTH SCIENCE > ") != 0) {
                csv_ignore_set.emplace(lparts.back());
              } else {
                if (args.concept_scheme == "platforms") {
                  string platform_type;
                  for (const auto& re : search_re_list) {
                    string check_string;
                    for (size_t n = 0; n < std::get<1>(re); ++n) {
                      if (!check_string.empty()) {
                        check_string += ",";
                      }
                      check_string += lparts[n];
                    }
                    if (regex_search(check_string, std::get<2>(re))) {
                      platform_type = std::get<3>(re);
                      break;
                    }
                  }
                  csv_map.emplace(lparts.back(), make_tuple(lparts[last], path,
                      platform_type));
                } else {
                  csv_map.emplace(lparts.back(), make_tuple(lparts[last], path,
                      ""));
                }
              }
            } else {
              csv_ignore_set.emplace(lparts.back());
            }
          }
        } else {
          cerr << "SKIPPING DUPLICATE: " << lparts.back() << endl;
        }
      }
    }
  }
  string kms_url = KMS_API_URL + "/concepts/concept_scheme/";
  if (!args.alias.empty()) {
    kms_url += args.alias;
  } else {
    kms_url += args.concept_scheme;
  }
  kms_url += "?format=xml&page_num=";
  auto page_num = 1;
  while (1) {
    mysystem2("/bin/sh -c 'curl -k -s -S -o - \"" + kms_url + to_string(
        page_num) + "\"'", oss, ess);
    if (!ess.str().empty()) {
      cerr << "Error getting concepts: " << ess.str() << endl;
      exit(1);
    }
    auto idx = oss.str().find("<concepts");
    if (idx == string::npos) {
      if (page_num == 1) {
        cerr << "Error in concepts xml - unable to find <concepts> element" <<
            endl;
        exit(1);
      } else {
        break;
      }
    }
    XMLSnippet concepts(oss.str().substr(oss.str().find("<concepts")));
    if (concepts) {
      string xpath = "concepts/conceptBrief@conceptScheme=" + args.
          concept_scheme;
      if (std::get<3>(args.concept_schemes[args.concept_scheme])) {
        auto elist = concepts.element_list(xpath + "@isLeaf=false");
        for (const auto& e : elist) {
          auto p = csv_map.find(e.attribute_value("uuid"));
          if (p != csv_map.end()) {
            csv_map.erase(p);
          }
        }
        xpath += "@isLeaf=true";
      }
      auto elist = concepts.element_list(xpath);
      if (elist.empty()) {
        cerr << "Aborting. /kms/concepts/concept_scheme/" << args.
            concept_scheme << "?format=xml may be corrupted." << endl;
        return;
      }
      for (const auto& e : elist) {
        auto p = csv_map.find(e.attribute_value("uuid"));
        if (p == csv_map.end()) {
          if (csv_ignore_set.find(e.attribute_value("uuid")) == csv_ignore_set.
              end()) {
            cerr << "MISSING UUID: " << e.attribute_value("uuid") << " (found '"
                << e.attribute_value("prefLabel") << "' in concepts, but not "
                "csv file)" << endl;
          }
        } else {
          string column_list, values_list;
          if (args.concept_scheme == "platforms") {
            column_list = "(uuid, last_in_path, path, obml_platform_type, "
                "cflag, dflag)";
            values_list = "'" + p->first + "', '" + sql_ready(std::get<0>(p->
                second)) + "', '" + sql_ready(std::get<1>(p->second)) + "', '" +
                sql_ready(std::get<2>(p->second)) + "', DEFAULT, '" + args.
                delete_flag + "'";
          } else {
            column_list = "(uuid, last_in_path, path, cflag, dflag)";
            values_list = "'" + p->first + "', '" + sql_ready(std::get<0>(p->
                second)) + "', '" + sql_ready(std::get<1>(p->second)) +
                "', DEFAULT, '" + args.delete_flag + "'";
          }
          string result;

// patch until all table names have been changed
auto tbl = "GCMD_" + args.concept_scheme;
if (!MySQL::table_exists(args.server, "search." + tbl)) {
tbl = "gcmd_" + args.concept_scheme;
}
//          if (args.server.command("insert into search.GCMD_" + args.
if (args.server.command("insert into search." + tbl
//              concept_scheme + " " + column_list + " values (" + values_list +
+ " " + column_list + " values (" + values_list +
              ") on duplicate key update cflag=if(cflag=9, 9, if(last_in_path="
              "values(last_in_path) and path=values(path), 0, 2)), last_in_path"
              "=values(last_in_path), path=values(path), dflag=values(dflag)",
              result) < 0) {
            cerr << "Error while inserting " << p->first << endl;
            cerr << args.server.error() << endl;
            exit(1);
          }
          csv_map.erase(p);
        }
      }
    } else {
      cerr << "Error parsing concepts: " << concepts.parse_error() << endl;
    }
    ++page_num;
  }
  string result;
  if (args.server.command("insert into search.gcmd_versions values ('" + args.
      concept_scheme + "', '" + version + "', '" + revision_date + "') on "
      "duplicate key update version=values(version), revision_date=values("
      "revision_date)", result) < 0) {
    cerr << "Error updating version number" << endl;
    cerr << args.server.error() << endl;
    exit(1);
  }
  if (!csv_map.empty()) {
    cout << "**Warning: " << csv_map.size() << " CSV keywords were not exposed "
        "by the API" << endl;
    for (const auto& e : csv_map) {
      cout << "    " << e.first << " " << std::get<1>(e.second) << endl;
    }
  }
  cout << "...update of '" << args.concept_scheme << "' complete at " <<
      dateutils::current_date_time().to_string() << endl;
}

void clean_keywords() {

// patch until all table names have been changed
auto tbl = "GCMD_" + args.concept_scheme;
if (!MySQL::table_exists(args.server, "search." + tbl)) {
tbl = "gcmd_" + args.concept_scheme;
}
//  MySQL::LocalQuery query("select uuid, path from search.GCMD_" + args.
MySQL::LocalQuery query("select uuid, path from search." + tbl
//      concept_scheme + " where dflag != '" + args.delete_flag + "' and uuid "
+ " where dflag != '" + args.delete_flag + "' and uuid "
      "not like 'RDA%'");
  if (query.submit(args.server) == 0) {
    if (query.num_rows() == 0) {
      cout << "No GCMD keywords were deleted in this update" << endl;
    } else {
      cout << query.num_rows() << " GCMD";
      if (query.num_rows() > 1) {
        cout << " keywords were";
      } else {
        cout << " keyword was";
      }
      cout << " deleted in this update:" << endl;
      for (const auto& row : query) {
        cout << "DELETED: " << row[0] << " '" << row[1] << "'" << endl;
      }
    }
  } else {
    cerr << "Error: unable to check for deleted keywords" << endl;
    cerr << query.error() << endl;
    exit(1);
  }
  args.server._delete("search.GCMD_" + args.concept_scheme, "dflag != '" + args.
      delete_flag + "' and uuid not like 'RDA%'");
}

void show_changed_keywords() {

// patch until all table names have been changed
auto tbl = "GCMD_" + args.concept_scheme;
if (!MySQL::table_exists(args.server, "search." + tbl)) {
tbl = "gcmd_" + args.concept_scheme;
}
//  MySQL::LocalQuery query("select uuid, path from search.GCMD_" + args.
MySQL::LocalQuery query("select uuid, path from search." + tbl
//      concept_scheme + " where cflag = 2 and uuid not like 'RDA%'");
+ " where cflag = 2 and uuid not like 'RDA%'");
  if (query.submit(args.server) == 0) {
    if (query.num_rows() == 0) {
      cout << "No GCMD keywords were changed in this update" << endl;
    } else {
      cout << query.num_rows() << " GCMD";
      if (query.num_rows() > 1) {
        cout << " keywords were";
      } else {
        cout << " keyword was";
      }
      cout << " changed in this update:" << endl;
      for (const auto& row : query) {
        cout << "CHANGED: " << row[0] << " '" << row[1] << "'" << endl;
      }
    }
  } else {
    cerr << "Error: unable to check for changed keywords" << endl;
    cerr << query.error() << endl;
    exit(1);
  }
//  query.set("select uuid, path from search.GCMD_" + args.concept_scheme +
query.set("select uuid, path from search." + tbl +
      " where cflag = 1 and uuid not like 'RDA%'");
  if (query.submit(args.server) == 0) {
    if (query.num_rows() == 0) {
      cout << "No GCMD keywords were added in this update" << endl;
    } else {
      cout << query.num_rows() << " GCMD";
      if (query.num_rows() > 1) {
        cout << " keywords were";
      } else {
        cout << " keyword was";
      }
      cout << " added in this update:" << endl;
      for (const auto& row : query) {
        cout << "ADDED: " << row[0] << " '" << row[1] << "'" << endl;
      }
    }
  } else {
    cerr << "Error: unable to check for added keywords" << endl;
    cerr << query.error() << endl;
    exit(1);
  }
}

void update_rda_keywords() {

// patch until all table names have been changed
auto tbl = "GCMD_" + args.concept_scheme;
if (!MySQL::table_exists(args.server, "search." + tbl)) {
tbl = "gcmd_" + args.concept_scheme;
}
  for (auto p : args.rda_keywords) {
    if (p.second == 1) {

      // add the keyword
      auto parts = split(p.first, " > ");
      string result;
//      if (args.server.command("insert into search.GCMD_" + args.concept_scheme +
if (args.server.command("insert into search." + tbl +
          " (uuid, last_in_path, path, cflag, dflag) values ('RDA" + strutils::
          uuid_gen().substr(3) + "', '" + sql_ready(parts.back()) + "', '" +
          sql_ready(p.first) + "', DEFAULT, '" + args.delete_flag + "') on "
          "duplicate key update cflag=if(cflag=9, 9, if(last_in_path=values("
          "last_in_path) and path=values(path), 0, 2)), last_in_path=values("
          "last_in_path), path=values(path), dflag=values(dflag)", result) <
          0) {
        cerr << "Error while inserting " << p.first << endl;
        cerr << args.server.error() << endl;
        exit(1);
      }
    }
  }
//  MySQL::LocalQuery query("select uuid, path from search.GCMD_" + args.
MySQL::LocalQuery query("select uuid, path from search." + tbl
//      concept_scheme + " where cflag = 1 and dflag = '" + args.delete_flag +
+ " where cflag = 1 and dflag = '" + args.delete_flag +
      "' and uuid like 'RDA%'");
  if (query.submit(args.server) == 0) {
    if (query.num_rows() == 0) {
      cout << "No RDA keywords were added in this update" << endl;
    } else {
      cout << query.num_rows() << " RDA";
      if (query.num_rows() > 1) {
        cout << " keywords were";
      } else {
        cout << " keyword was";
      }
      cout << " added in this update:" << endl;
      for (const auto& row : query) {
        cout << "ADDED RDA KEYWORD: " << row[0] << " '" << row[1] << "'" <<
            endl;
      }
    }
  } else {
    cerr << "Error: unable to check for added RDA keywords" << endl;
    cerr << query.error() << endl;
    exit(1);
  }
}

void show_orphaned_keywords() {
  vector<tuple<string, string>> keyword_tables{
    make_tuple("variables", "gcmd_sciencekeywords"),
    make_tuple("platforms_new", "gcmd_platforms"),
    make_tuple("contributors_new", "gcmd_providers"),
    make_tuple("instruments", "gcmd_instruments"),
    make_tuple("projects_new", "gcmd_projects"),
    make_tuple("supportedProjects_new", "gcmd_projects")
  };
  for (const auto& map : keyword_tables) {
    MySQL::LocalQuery query("select distinct keyword, dsid from search." + std::
        get<0>(map) + " as k left join search." + std::get<1>(map) + " as g on "
        "g.uuid = k.keyword where k.vocabulary = 'GCMD' and isnull(g.uuid) "
        "order by dsid, keyword");
    if (query.submit(args.server) == 0) {
      for (const auto& row : query) {
        cout << "ORPHANED KEYWORD: " << row[0] << " in " << row[1] << " (" <<
            std::get<0>(map) << ")" << endl;
      }
    } else {
      cerr << "Database error for query '" << query.show() << "': '" << query.
          error() << "'" << endl;
    }
  }
}

int main(int argc, char **argv) {
  parse_args(argc, argv);
  if (!args.no_gcmd_update) {
    vector<string> concept_schemes;
    if (args.concept_scheme == "all") {
      for (const auto& s : args.concept_schemes) {
        concept_schemes.emplace_back(s.first);
      }
    } else {
      concept_schemes.emplace_back(args.concept_scheme);
    }
    for (const auto& s : concept_schemes) {
      args.concept_scheme = s;
      if (passed_date_check()) {
        update_keywords();
        clean_keywords();
        show_changed_keywords();
      }
    }
  }
  update_rda_keywords();
  if (!args.no_gcmd_update) {
    show_orphaned_keywords();
  }
}
