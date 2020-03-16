
using System.Collections.Generic;

namespace Generator
{
    public class NamingCleanup
    {
        public static void CleanColumnNameQuantum(DatabaseColumn column)
        {
            string databaseColumnName = column.DatabaseColumnName.ToLower();

            if (databaseColumnName == "id")
            {
                if (column.DatabaseObjectName == "cc_match")
                    databaseColumnName = "cash_set_number"; 
                else
                {
                    databaseColumnName = RenameObject(column.DatabaseObjectName) + "_id";
                }
            }
            else
            {
                if (column.DatabaseObjectName == "adjuster" && databaseColumnName == "name")
                    databaseColumnName = "adjuster_name";

                else if (column.DatabaseObjectName == "comp" && databaseColumnName == "claim_comp_grp")
                    databaseColumnName = "company_group";

                else
                {
                    // remove lloyds before
                    if (column.DatabaseObjectName == "pol_main_detl" && databaseColumnName.StartsWith("lloyds_"))
                        databaseColumnName = databaseColumnName.Substring(7, databaseColumnName.Length - 7);

                    databaseColumnName = RenameObject(databaseColumnName);
                }
            }
            column.TargetColumnName = databaseColumnName.ToCamelCaseReplacingUnderscores();
        }

        public static void CleanDatabaseObjectName(DatabaseObject databaseObject)
        {
            string objectName = databaseObject.DatabaseObjectName.ToLower();

            objectName = RenameObject(objectName);

            databaseObject.TargetObjectName = objectName.ToCamelCaseReplacingUnderscores();
        }

        private static string RenameObject(string databaseColumnName)
        {
            // whole string matches
            Dictionary<string, string> wholeStringReplacements = new Dictionary<string, string>();
            wholeStringReplacements.Add("email_contact_4", "vat_registration_number"); 
            wholeStringReplacements.Add("adj_perd", "adjustment_period_day_count");
            wholeStringReplacements.Add("agg_exp_amt", "aggregate_exposure_amount");
            wholeStringReplacements.Add("agg_exp_curr_cd", "aggregate_exposure_currency_code");
            wholeStringReplacements.Add("brokercontact", "broker_contact");
            wholeStringReplacements.Add("cc_match_audit_no", "cash_set_number");
            wholeStringReplacements.Add("chg_in_rsk_pct", "rate_change_risk_percentage");
            wholeStringReplacements.Add("chg_in_rte_pct", "rate_change_rate_percentage");
            wholeStringReplacements.Add("chng_in_prem_rt_rtbl", "rate_change_premium_rate");
            wholeStringReplacements.Add("claim_ref", "cedant_claim_reference");
            wholeStringReplacements.Add("contracttype", "contract_type");
            wholeStringReplacements.Add("instal_eq_unq", "equal_unequal_instalments");
            wholeStringReplacements.Add("pol_uw_yr", "uw_yr"); // remove policy
            wholeStringReplacements.Add("tty_desc", "policy_description");
            wholeStringReplacements.Add("tty_sta_cd", "status_code");
            wholeStringReplacements.Add("tty_typ_cd", "policy_type_code");
            wholeStringReplacements.Add("uw_init", "underwriter_initials");
            wholeStringReplacements.Add("uw_nm", "underwriter_name");
            wholeStringReplacements.Add("wrt_order_signed_dt", "written_date");
            wholeStringReplacements.Add("ctry_orig", "country_of_origin");
            wholeStringReplacements.Add("ntm", "new_to_market");
            wholeStringReplacements.Add("tsi", "total_sum_insured");
            wholeStringReplacements.Add("pbi", "primary_business_indicator");
            wholeStringReplacements.Add("indexation", "indexation_code");
            wholeStringReplacements.Add("acct_cd", "office_code"); 
            wholeStringReplacements.Add("risk_exp_cd", "risk_exposure_code");
            wholeStringReplacements.Add("eer", "extraordinary_event_report");
            //wholeStringReplacements.Add("acct_type_desc", "account_name");

            foreach (KeyValuePair<string, string> kvp in wholeStringReplacements)
            {
                if (kvp.Key == databaseColumnName) databaseColumnName = kvp.Value;
            }

            Dictionary <string, string> replacementTokens = new Dictionary<string, string>();
            replacementTokens.Add("abs", "absolute");
            replacementTokens.Add("acc", "account");
            replacementTokens.Add("acct", "account");
            replacementTokens.Add("addr", "address");
            replacementTokens.Add("adj", "adjustment");
            replacementTokens.Add("adv", "advance");
            replacementTokens.Add("advis", "advised");
            replacementTokens.Add("agg", "aggregate");
            replacementTokens.Add("alloc", "allocation");
            replacementTokens.Add("alt", "alternative");
            replacementTokens.Add("amt", "amount");
            replacementTokens.Add("ap", "addition_premium");
            replacementTokens.Add("annl", "annual");
            replacementTokens.Add("bdown", "breakdown");
            replacementTokens.Add("bio", "biological");
            replacementTokens.Add("bi", "business_interruption");
            replacementTokens.Add("bkd", "booked");
            replacementTokens.Add("bordxfreq", "bordereaux_frequency");
            replacementTokens.Add("brk", "broker");
            replacementTokens.Add("brkage", "brokerage");
            replacementTokens.Add("brkg", "brokerage");
            replacementTokens.Add("brok", "broker");
            replacementTokens.Add("bus", "business");

            replacementTokens.Add("canc", "cancelled");
            replacementTokens.Add("cat", "category");
            replacementTokens.Add("catg", "catalog");
            replacementTokens.Add("cc", "contract_certainty");
            replacementTokens.Add("ccy", "currency");
            replacementTokens.Add("cd", "code");
            replacementTokens.Add("ced", "cedant");
            replacementTokens.Add("cert", "certainty");
            replacementTokens.Add("chem", "chemical");
            replacementTokens.Add("chng", "change");
            replacementTokens.Add("chg", "change");
            replacementTokens.Add("chrg", "charge");
            replacementTokens.Add("clm", "claim");
            replacementTokens.Add("clos", "close");
            replacementTokens.Add("cnt", "count");
            replacementTokens.Add("col", "column");
            replacementTokens.Add("comm", "commission");
            replacementTokens.Add("commut", "commuted");
            replacementTokens.Add("comp", "company");
            replacementTokens.Add("cond", "conditions");
            replacementTokens.Add("cred", "credit_days");
            replacementTokens.Add("ctrl", "control");
            replacementTokens.Add("ctry", "country");
            replacementTokens.Add("curr", "currency");
            replacementTokens.Add("cust", "customer");
            replacementTokens.Add("cyb", "cyber");

            replacementTokens.Add("datalib", "data_library");
            replacementTokens.Add("ded", "deductable");
            replacementTokens.Add("deduct", "deductions");
            replacementTokens.Add("def", "deferred");
            replacementTokens.Add("defd", "deferred");
            replacementTokens.Add("dep", "deposit");
            replacementTokens.Add("dept", "department");
            replacementTokens.Add("desc", "description");
            replacementTokens.Add("descr", "description");
            replacementTokens.Add("decl", "declined");
            replacementTokens.Add("detl", "detail");
            replacementTokens.Add("dom", "domicile");
            replacementTokens.Add("dt", "date");

            replacementTokens.Add("ent", "entry");
            replacementTokens.Add("eff", "effective");
            replacementTokens.Add("equiv", "equivalent");
            replacementTokens.Add("err", "error");
            replacementTokens.Add("est", "estimated");
            replacementTokens.Add("exch", "exchange");
            replacementTokens.Add("excl", "excluding");
            replacementTokens.Add("exp", "expiry");
            replacementTokens.Add("expos", "exposure");

            replacementTokens.Add("fin", "financial");
            replacementTokens.Add("fluc", "fluctuation");
            replacementTokens.Add("fgu", "from_ground_up");
            replacementTokens.Add("fgu2", "from_ground_up2");
            replacementTokens.Add("fgu3", "from_ground_up3");
            replacementTokens.Add("font", "fronting");

            replacementTokens.Add("grp", "group");
            replacementTokens.Add("gn", "gross_net");

            replacementTokens.Add("hist", "history");
            replacementTokens.Add("hold", "holding");
            replacementTokens.Add("hse", "house");

            replacementTokens.Add("ilw", "industry_loss_warranty");
            replacementTokens.Add("inc", "inclusion");
            replacementTokens.Add("incept", "inception");
            replacementTokens.Add("ind", "indicator");
            replacementTokens.Add("instal", "instalments");
            replacementTokens.Add("in", "inward");
            replacementTokens.Add("inw", "inward");
            replacementTokens.Add("ins", "insured");

            replacementTokens.Add("ldr", "leader");
            replacementTokens.Add("lim", "limit");
            replacementTokens.Add("ll", "lloyds");
            replacementTokens.Add("ln", "line");
            replacementTokens.Add("lob", "line_of_business");
            replacementTokens.Add("loc", "location");
            replacementTokens.Add("lsrp", "lloyds_srp");
            replacementTokens.Add("lt", "long_term");
            replacementTokens.Add("lvl", "level");

            replacementTokens.Add("maj", "major");
            replacementTokens.Add("max", "maximum");
            replacementTokens.Add("md", "minimum_deposit");
            replacementTokens.Add("method", "method_of");
            replacementTokens.Add("min", "minimum");
            replacementTokens.Add("mkt", "market");
            replacementTokens.Add("mnm", "manager");
            replacementTokens.Add("mth", "month");
            replacementTokens.Add("mult", "multi");

            replacementTokens.Add("narr", "narrative");
            replacementTokens.Add("natn", "exposed_territory");
            replacementTokens.Add("neg", "negative");
            replacementTokens.Add("nm", "name");
            replacementTokens.Add("nm2", "name2");
            replacementTokens.Add("ntm", "new_to_market");
            replacementTokens.Add("no", "number");
            replacementTokens.Add("noc", "notice");
            replacementTokens.Add("ntu", "not_taken_up");
            replacementTokens.Add("nuc", "nuclear");

            replacementTokens.Add("occ", "occurrence");
            replacementTokens.Add("olw", "original_loss_warranty");
            replacementTokens.Add("opp", "original_policy_period");
            replacementTokens.Add("orig", "original");
            replacementTokens.Add("oth", "other");
            replacementTokens.Add("out", "outward");
            replacementTokens.Add("outw", "outward");
            replacementTokens.Add("ovrdr", "override");

            replacementTokens.Add("pay", "payment");
            replacementTokens.Add("pct", "percentage");
            replacementTokens.Add("perd", "period");
            replacementTokens.Add("plc", "placing");
            replacementTokens.Add("pml", "possible_maximum_loss");
            replacementTokens.Add("pnoc", "provisional_notice_of_cancellation");
            replacementTokens.Add("pol", "policy");
            replacementTokens.Add("pol_uw", "underwriting");
            replacementTokens.Add("ppo", "periodical_payment_order");
            replacementTokens.Add("pr", "peer_review");
            replacementTokens.Add("prem", "premium");
            replacementTokens.Add("prod", "placing");
            replacementTokens.Add("pbi", "primary_business_indicator");
            replacementTokens.Add("pd", "property_damage");
            replacementTokens.Add("prof", "profit");
            replacementTokens.Add("prog", "program");
            replacementTokens.Add("prop", "property");
            replacementTokens.Add("prorata", "pro_rata");
            replacementTokens.Add("purch", "purchase");

            replacementTokens.Add("qtr", "quarter");

            replacementTokens.Add("rad", "radiation");
            replacementTokens.Add("rec", "recovery");
            replacementTokens.Add("recv", "received");
            replacementTokens.Add("recvd", "received");
            replacementTokens.Add("ref", "reference");
            replacementTokens.Add("rein", "reinstatement");
            replacementTokens.Add("reins", "reinsurer");
            replacementTokens.Add("reinst", "reinstatement");
            replacementTokens.Add("renew", "renewed");
            replacementTokens.Add("reqd", "required");
            replacementTokens.Add("resp", "response");
            replacementTokens.Add("rev", "revised");
            replacementTokens.Add("ri", "reinsurance");
            replacementTokens.Add("rol", "rate_on_line");
            replacementTokens.Add("rsk", "risk");
            replacementTokens.Add("rt", "rate");
            replacementTokens.Add("rtbl", "");
            replacementTokens.Add("rte", "rate");

            replacementTokens.Add("sect", "section");
            replacementTokens.Add("seq", "sequence");
            replacementTokens.Add("sett", "settlement");
            replacementTokens.Add("settl", "settlement");
            replacementTokens.Add("settle", "settled");
            replacementTokens.Add("si", "sum_insured");
            replacementTokens.Add("sgn", "signed");
            replacementTokens.Add("shr", "share");
            replacementTokens.Add("solv", "solvency");
            replacementTokens.Add("sml", "short_medium_long");
            replacementTokens.Add("sta", "status");
            replacementTokens.Add("stat", "status");
            replacementTokens.Add("std", "standard");
            replacementTokens.Add("sumins", "sum_insured");
            replacementTokens.Add("sens", "sensitive");
            replacementTokens.Add("swbk", "switched_to_booked");
            replacementTokens.Add("ppw", "written_premium_pattern");

            replacementTokens.Add("terr", "territory");
            replacementTokens.Add("tot", "terms_of_trade");
            replacementTokens.Add("trad", "traditional");
            replacementTokens.Add("tran", "transaction");
            replacementTokens.Add("trig", "trigger");
            replacementTokens.Add("tty", "treaty");
            replacementTokens.Add("tsfr", "transfer");
            replacementTokens.Add("tsi", "total_sum_insured");
            replacementTokens.Add("typ", "type");

            replacementTokens.Add("ucmr", "unique_claim_reference");
            replacementTokens.Add("ulr", "loss_ratio");
            replacementTokens.Add("ult", "ultimate");
            replacementTokens.Add("umr", "unique_market_reference");
            replacementTokens.Add("uw", "underwriting");
            replacementTokens.Add("uwbase", "underwriting_base");
            replacementTokens.Add("uwexpected", "underwriting_expected");

            replacementTokens.Add("val", "value");

            replacementTokens.Add("wnty", "warranty");
            replacementTokens.Add("wrt", "written");
            replacementTokens.Add("xs", "excess");
            replacementTokens.Add("yr", "year");

            foreach (KeyValuePair<string,string> kvp in replacementTokens)
            {
                databaseColumnName = ReplaceToken(databaseColumnName, kvp.Key, kvp.Value);
            }

            // fix some issues which the above creates
            databaseColumnName = ReplaceTokenAtStartOfName(databaseColumnName, "change_inward_", "change_in_");
            databaseColumnName = ReplaceTokenAtStartOfName(databaseColumnName, "lod_radiation_", "lod_rad_");

            Dictionary<string, string> fixStringReplacements = new Dictionary<string, string>();
            fixStringReplacements.Add("number_claim_bonus_percentage", "no_claim_bonus_percentage");
            fixStringReplacements.Add("category_guarantee_code", "catastrophe_guarantee_code");
            fixStringReplacements.Add("payment_inward_advance", "payment_in_advance");
            foreach (KeyValuePair<string, string> kvp in fixStringReplacements)
            {
                if (kvp.Key == databaseColumnName) databaseColumnName = kvp.Value;
            }
            
            return databaseColumnName;
        }

        private static string ReplaceToken(string name, string token, string replacement)
        {
            string newToken = token + "_";
            string newReplacement = replacement + "_";
            name = ReplaceTokenAtStartOfName(name, newToken, newReplacement);

            newToken = "_" + token + "_";
            newReplacement = "_" + replacement + "_";
            name = ReplaceTokenInName(name, newToken, newReplacement);

            newToken = "_" + token;
            newReplacement = "_" + replacement;
            name = ReplaceTokenAtEndOfName(name, newToken, newReplacement);

            return name;
        }


        private static string ReplaceTokenAtStartOfName(string name, string token, string replacement)
        {
            int len = token.Length;
            if (name.StartsWith(token) && name.Length > len) 
            {

                int pos = name.IndexOf(token);

                name = name.Substring(0, pos) + replacement + name.Substring(pos + len, name.Length - pos - len);
            }
            return name;

        }

        private static string ReplaceTokenAtEndOfName(string name, string token, string replacement)
        {
            int len = token.Length;
            if (name.EndsWith(token) && name.Length > len)
            {

                int pos = name.LastIndexOf(token);

                name = name.Substring(0, pos) + replacement + name.Substring(pos + len, name.Length - pos - len);
            }
            return name;

        }
        private static string ReplaceTokenInName(string name, string token, string replacement)
        {
            int len = token.Length;
            if (name.Contains(token) && name.Length > len)
            {

                int pos = name.IndexOf(token);

                name = name.Substring(0, pos) + replacement + name.Substring(pos + len, name.Length - pos - len);
            }
            return name;
        }
    }
}
