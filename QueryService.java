package ubergloss.services;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.*;

import ubergloss.connectors.AbstractConnector;
import ubergloss.objects.Definition;
import ubergloss.objects.Filter;
import ubergloss.objects.Filter.FilterTypes;
import ubergloss.objects.Locale;
import ubergloss.objects.SearchResult;
import ubergloss.objects.Tag;

public class QueryService {

	private static final int LEVENSHTEIN_DISTANCE = 3;
	
	// query to search for a term in the database
	protected CallableStatement searchForTerm;
	protected PreparedStatement searchInDefinition;
	protected PreparedStatement searchInTerm;

	private AbstractConnector connection;
	private DefinitionService defServ;
	private TagService tagserv;
	private LocaleService locserv;

	public QueryService(AbstractConnector conn, DefinitionService defServ) {
		if (conn == null || defServ == null)
			throw new IllegalArgumentException(
					"the connection or definition service must be non-null.");
		try {
			searchForTerm = conn.getDBConnection().prepareCall(
					"{CALL SEARCH(?,?)}");

			searchInDefinition = conn
					.getDBConnection()
					.prepareStatement(
							"SELECT definitions.defID, rank, definition, termdef.term FROM definitions, termdef WHERE definition LIKE ? AND termdef.defID = definitions.defID;");
			searchInTerm = conn.getDBConnection().prepareStatement(
					"SELECT definitions.defID, rank, definition, termdef.term FROM definitions, termdef WHERE termdef.term LIKE ? AND termdef.defID = definitions.defID;");
		
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		connection = conn;
		this.defServ = defServ;
		locserv = new LocaleService(conn);
		tagserv = new TagService(conn);
	}

	/**
	 * This class acts as a container for all of the information associated with
	 * a definition, including locales and tags. It's purpose is to reduce the 
	 * number of database queries.
	 * 
	 * @author George Kinsman
	 * 
	 */
	public class CompleteDefinition {
		private Definition def = null;
		private List<Tag> tags = null;
		private List<Locale> locales = null;

		public CompleteDefinition(Definition def, List<Tag> tags,
				List<Locale> locales) {
			this.setDefinition(def);
			this.setTags(tags);
			this.setLocales(locales);
		}

		public void setDefinition(Definition def) {
			this.def = def;
		}

		public Definition getDefinition() {
			return def;
		}

		public void setTags(List<Tag> tags) {
			this.tags = tags;
		}

		public List<Tag> getTags() {
			return tags;
		}

		public void setLocales(List<Locale> locales) {
			this.locales = locales;
		}

		public List<Locale> getLocales() {
			return locales;
		}

		public boolean equals(Object o) {
			if (((Definition) o).equals(this.def)) {
				return true;
			} else {
				return false;
			}
		}

	}

	/**
	 * Accepts a list of filters and performs the query
	 * 
	 * @param query
	 *            - the UAL phrase to search for
	 * @return a list of definitions that contain all UAL filters
	 */
	public Set<Definition> performSearch(Set<Filter> filters) {
		Set<Definition> searchResults;

		// 2. get maximum result set
		Set<Definition> maxResultSet = getMaximumResultSet(filters);

		// 3. get complete definitions
		List<CompleteDefinition> completeDefinitions = getCompleteDefinitions(maxResultSet);

		// 4. filter results
		searchResults = filterResults(completeDefinitions, filters);
		
		
		
		return searchResults;
	}

	/**
	 * This method performs all parsing on the query string and decides what the
	 * user wants to do. It uses other methods to perform the actual searching
	 * 
	 * @param The
	 *            entire search query string
	 * @return a set of filters parsed from the query
	 */
	public Set<Filter> parseQuery(String query) {
		List<Pattern> patterns = new ArrayList<Pattern>();

		// Definition pattern format: "search term"
		Pattern definition = Pattern.compile("\".*?\"");
		// Tag pattern format: [tag] eg. [science]
		Pattern tag = Pattern.compile("\\[[^]\\r\\n]+]");
		// Locale pattern format: (locale) eg. (en-AU)
		Pattern locale = Pattern.compile("\\([^]\\r\\n\\s]*\\)");
		Pattern term = Pattern.compile("(?<=^|\\s)[a-zA-Z]+(?=\\s|$)");

		patterns.add(definition);
		patterns.add(tag);
		patterns.add(locale);
		patterns.add(term);

		Set<String> filters = new HashSet<String>();

		// Go through each pattern and check for a match
		for (Pattern pat : patterns) {

			Matcher match = pat.matcher(query);

			while (match.find()) {
				String filter = query.substring(match.start(), match.end());
				filters.add(filter);
			}
		}
		Set<Filter> returnFilters = convertStringsToFilters(filters);
		
		return validateFilters(returnFilters);
	}

	/**
	 * Returns a set of filters parsed from their string equivalents
	 * 
	 * @param filter
	 *            A set of filters to search for in raw string form
	 */
	public Set<Filter> convertStringsToFilters(Set<String> filters) {

		Set<Filter> realFilters = new HashSet<Filter>();

		for (String filter : filters) {
			if (filter.startsWith("[")) {
				realFilters.add(new Filter(FilterTypes.Tag, filter.substring(1,
						filter.length() - 1)));

			} else if (filter.startsWith("\"")) {
				realFilters.add(new Filter(FilterTypes.Definition, filter
						.substring(1, filter.length() - 1)));

			} else if (filter.startsWith("(")) {
				realFilters.add(new Filter(FilterTypes.Locale, filter
						.substring(1, filter.length() - 1)));

			} else {
				realFilters.add(new Filter(FilterTypes.Term, filter));
			}
		}
		return realFilters;
	}
	
	
	private Set<Filter> validateFilters(Set<Filter> filters) {
		
		for(Filter filter : filters) {
			if(filter.getType() == FilterTypes.Tag) {
				if(tagserv.tagExists(filter.getQuery())) {
					filter.setVerified(true);
				}
			} else if(filter.getType() == FilterTypes.Locale) {
				if(locserv.localeExists(filter.getQuery())) {
					filter.setVerified(true);
				}
			}
		}
		
		return filters;
	}

	/**
	 * Queries the respective services to get all information associated with
	 * each definition
	 * 
	 * @param defs
	 * @return
	 */
	public List<CompleteDefinition> getCompleteDefinitions(Set<Definition> defs) {

		TagService tserv = new TagService(connection);
		LocaleService lserv = new LocaleService(connection);

		List<CompleteDefinition> results = new ArrayList<CompleteDefinition>();

		for (Definition def : defs) {
			results.add(new CompleteDefinition(def, 
					tserv.getTagsForDefinition(def.getID()), 
					lserv.getLocalesForDefinition(def.getID())));
		}

		return results;
	}

	/**
	 * Returns the largest possible list of definitions possible from the
	 * database, without duplicates
	 * 
	 * For each filter, fetches the definitions that contain the filter
	 * 
	 * @param filters
	 *            a set of filters to search for
	 * @return a maximal set of definitions from the database
	 */
	public Set<Definition> getMaximumResultSet(Set<Filter> filters) {
		Set<Definition> definitions = new HashSet<Definition>();

		for (Filter filter : filters) {
			if (filter.getType() == FilterTypes.Definition) {
				definitions.addAll(definitionSearch(filter.getQuery()));
			} else if (filter.getType() == FilterTypes.Locale) {
				definitions.addAll(locserv.getDefinitionsForLocale(filter
						.getQuery()));
			} else if (filter.getType() == FilterTypes.Tag) {
				definitions.addAll(tagserv.getDefinitionsTaggedWith(filter
						.getQuery()));
				
			} else {
				definitions.addAll(levenshtein(filter.getQuery(), 
						LEVENSHTEIN_DISTANCE));
				//definitions.addAll(termSearch(filter.getQuery()));
			}
		}

		return definitions;
	}

	/**
	 * Accepts a list of CompleteDefinition's and a set of filters, and filters
	 * the list to only include those definitions that pass every filter
	 * 
	 * @param defs
	 * @param filters
	 * @return
	 */
	public Set<Definition> filterResults(List<CompleteDefinition> defs,
			Set<Filter> filters) {

		Set<CompleteDefinition> results = new HashSet<CompleteDefinition>(defs);

		for (CompleteDefinition def : defs) {
			for (Filter filter : filters) {
				if (filter.getType() == FilterTypes.Tag) {
					if (!checkTags(def.getTags(), filter.getQuery())) {
						results.remove(def);
						break;
					}
				} else if (filter.getType() == FilterTypes.Definition) {
					if (!def.getDefinition().getDefinition()
							.contains(filter.getQuery())) {
						results.remove(def);
						break;
					}
				} else if (filter.getType() == FilterTypes.Locale) {
					if (!checkLocales(def.getLocales(), filter.getQuery())) {
						results.remove(def);
						break;
					}
				} else if(filter.getType() == FilterTypes.Term) {
					if((levenshtein(def.getDefinition().getTerm(), filter.getQuery()) > LEVENSHTEIN_DISTANCE) ||
							!def.getDefinition().getTerm().contains(filter.getQuery())) {
						results.remove(def);
						break;
					}
				}
			}
		}

		return trimCompleteDefinitions(results);
	}

	/**
	 * Return a Set of Definitions without the additional data associated with
	 * CompleteDefinitions
	 * 
	 * @param defs
	 * @return
	 */
	private Set<Definition> trimCompleteDefinitions(Set<CompleteDefinition> defs) {
		Set<Definition> results = new HashSet<Definition>();

		for (CompleteDefinition def : defs) {
			results.add(def.getDefinition());
		}

		return results;
	}

	/**
	 * Checks to see if the given list of tags contains a tag with the specified
	 * tag name
	 * 
	 * @param tags
	 *            The list of tags to search in
	 * @param query
	 *            the name of the tag to search for in the list
	 * @return true if the list of tags contains the queried tag
	 */
	private Boolean checkTags(List<Tag> tags, String query) {

		for (Tag tag : tags) {
			if (tag.getName().equalsIgnoreCase(query)) {
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

	/**
	 * Checks to see if the given list of locales contains a locale with the
	 * specified short name
	 * 
	 * @param locales
	 *            The list of locales to search in
	 * @param shortName
	 *            The short name of the locale to search for
	 * @return true if the list of locales contains specified locale
	 */
	private Boolean checkLocales(List<Locale> locales, String shortName) {
		for (Locale loc : locales) {
			if (loc.getShortName().equalsIgnoreCase(shortName)) {
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

	/**
	 * Trims the filter metadata from the query string itself
	 * 
	 * @param filter
	 * @return
	 */
	private String trimFilter(String filter) {
		return filter.substring(1, filter.length() - 1);
	}

	/**
	 * Returns a list of definitions who's textual definitions contain all of
	 * the given string
	 * 
	 * @param query
	 *            - the string to search for in each definition
	 * @return a list of definitions containing the query string
	 */
	public List<Definition> definitionSearch(String query) {

		List<Definition> toReturn = new ArrayList<Definition>();

		try {
			searchInTerm.setString(1, "%" + query + "%");

			ResultSet rs = searchInDefinition.executeQuery();

			while (rs.next()) {
				toReturn.add(new Definition(
						rs.getString("term"), 
						rs.getString("definition"), 
						rs.getString("rank"), 
						rs.getString("defID")));
			}

		} catch (SQLException e) {
			toReturn = null;
		}
		return toReturn;
	}
	
	public List<Definition> termSearch(String query) {

		List<Definition> toReturn = new ArrayList<Definition>();

		try {
			searchInTerm.setString(1, "%" + query + "%");

			ResultSet rs = searchInTerm.executeQuery();

			while (rs.next()) {
				toReturn.add(new Definition(
						rs.getString("term"), 
						rs.getString("definition"), 
						rs.getString("rank"), 
						rs.getString("defID")));
			}

		} catch (SQLException e) {
			toReturn = null;
		}
		return toReturn;
	}

	/**
	 * Returns a list of Definitions 
	 * 
	 * @param term
	 * @param distance
	 * @return
	 */
	public List<Definition> levenshtein(String term, Integer distance) {

		if (term == null || term.equals(""))
			throw new IllegalArgumentException(
					"The supplied term must be non-null and non-empty.");

		List<Definition> toReturn = new ArrayList<Definition>();
		try {

			searchForTerm.setString(1, term);
			searchForTerm.setString(2, distance.toString());
			Boolean hadResults = searchForTerm.execute();

			if (hadResults) {
				ResultSet rs = searchForTerm.getResultSet();

				while (rs.next()) {
					Definition def = defServ.getFirstDefinition(rs
							.getString("term"));
					if (def != null) {
						Definition result = def;
								//Integer.parseInt(rs.getString("relevance")));

						toReturn.add(result);
					}
				}
			}
		} catch (SQLException e) {
			// return null to signal an error
			toReturn = null;
		}
		return toReturn;
	}

	/**
	 * Removes a filter from a set of filters and constructs a new URL string
	 * 
	 * @return
	 */
	public String removeFilterReturnURL(Set<Filter> filters, Filter toRemove) {
		if (filters.size() > 1) {

			filters.remove(toRemove);

			return filtersToString(filters,"+");
			
		} else {
			return null;
		}
	}
	
	public String filtersToString(Set<Filter> filters, String separator) {
		StringBuilder toReturn = new StringBuilder();
		
		for(Filter filter : filters) {
			toReturn.append(filter+separator);
		}
		String result = toReturn.toString();

		result = result.substring(0, result.lastIndexOf(separator));

		return result;
	}
	
	
	public String htmlEscape(String input) {
		return input.replace("&", "&amp;").
			 		 replace("<", "&lt;").
			 		 replace("\"", "&quot;").
			 		 replace(">", "&gt;");
	}

	/**
	 * Levenshtein algorithm from http://www.merriampark.com/ld.htm
	 * @param s
	 * @param t
	 * @return
	 */
	private static int levenshtein(String s, String t) {
		int d[][]; // matrix
		int n; // length of s
		int m; // length of t
		int i; // iterates through s
		int j; // iterates through t
		char s_i; // ith character of s
		char t_j; // jth character of t
		int cost; // cost

		// Step 1

		n = s.length();
		m = t.length();
		if (n == 0) {
			return m;
		}
		if (m == 0) {
			return n;
		}
		d = new int[n + 1][m + 1];

		// Step 2

		for (i = 0; i <= n; i++) {
			d[i][0] = i;
		}

		for (j = 0; j <= m; j++) {
			d[0][j] = j;
		}

		// Step 3

		for (i = 1; i <= n; i++) {

			s_i = s.charAt(i - 1);

			// Step 4

			for (j = 1; j <= m; j++) {

				t_j = t.charAt(j - 1);

				// Step 5

				if (s_i == t_j) {
					cost = 0;
				} else {
					cost = 1;
				}

				// Step 6

				d[i][j] = Minimum(d[i - 1][j] + 1, d[i][j - 1] + 1,
						d[i - 1][j - 1] + cost);

			}

		}

		// Step 7

		return d[n][m];
	}

	private static int Minimum(int a, int b, int c) {
		int mi;

		mi = a;
		if (b < mi) {
			mi = b;
		}
		if (c < mi) {
			mi = c;
		}
		return mi;

	}

}
