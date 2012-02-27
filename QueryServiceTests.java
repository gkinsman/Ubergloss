package ubergloss.serviceTests;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import ubergloss.connectors.AbstractConnector;
import ubergloss.services.DefinitionService;
import ubergloss.services.QueryService;
import ubergloss.services.QueryService.CompleteDefinition;
import ubergloss.objects.Definition;
import ubergloss.objects.Filter;
import ubergloss.objects.Filter.FilterTypes;

import org.junit.*;
import static org.junit.Assert.*;

public class QueryServiceTests {
	
	private DefinitionService ds;
	private QueryService qs;
	
	@Before
	public void setup() {
		ds = new DefinitionService(AbstractConnector.getConnector("admin",
		"password"));
		
		qs = new QueryService(AbstractConnector.getConnector("admin", "password"), ds);
	}
	
	@Test
	public void parseQuery_SingleTag_OneTask() {
		Filter filter = new Filter(FilterTypes.Tag, "tag");
		
		Set<Filter> tasks = qs.parseQuery("[tag]");
		assertTrue(tasks.contains(filter));
	}
	
	@Test
	public void parseQuery_MultipleTags_MultipleTasks() {
		Filter filter1 = new Filter(FilterTypes.Tag, "tag1");
		Filter filter2 = new Filter(FilterTypes.Tag, "tag2");
		Filter filter3 = new Filter(FilterTypes.Tag, "tag3");
		
		Set<Filter> tasks = qs.parseQuery("[tag3] [tag2] [tag1]");
		
		assertTrue(tasks.contains(filter1));
		assertTrue(tasks.contains(filter2));
		assertTrue(tasks.contains(filter3));
	}
	
	@Test
	public void parseQuery_DuplicateTags_NoDuplicateTasks() {
		Filter filter1 = new Filter(FilterTypes.Tag, "tag1");
		Filter filter2 = new Filter(FilterTypes.Tag, "tag2");
		
		Set<Filter> filters = qs.parseQuery("[tag1] [tag2] [tag2]");
		
		assertTrue(filters.contains(filter1));
		assertTrue(filters.contains(filter2));
		
		Integer taskDuplicateCount = 0;
		for(Filter filter : filters) {
			if(filter.equals(filter2)) {
				taskDuplicateCount++;
			}
		}
		assertTrue(taskDuplicateCount == 1);
	}
	
	@Test
	public void parseQuery_MultipleDuplicateTags_NoDuplicateTasks() {
		Filter filter1 = new Filter(FilterTypes.Tag, "tag1");
		Filter filter2 = new Filter(FilterTypes.Tag, "tag2");
		Filter filter3 = new Filter(FilterTypes.Tag, "tag3");
		Filter filter4 = new Filter(FilterTypes.Tag, "tag4");
		
		Set<Filter> filters = qs.parseQuery("[tag1] [tag1] [tag2] [tag1] [tag3] [tag4] [tag3])");
		
		Integer tag1DupCount = 0;
		Integer tag3DupCount = 0;
		
		for(Filter filter : filters) {
			if(filter.equals(filter1)) {
				tag1DupCount++;
			} else if(filter.equals(filter3)) {
				tag3DupCount++;
			}
		}
		
		assertTrue(tag1DupCount == 1);
		assertTrue(tag3DupCount == 1);
	}
	
	@Test
	public void parseQuery_SingleLocale_SingleTask() {
		Filter filter1 = new Filter(FilterTypes.Locale, "locale");
		
		Set<Filter> filters = qs.parseQuery("(locale)");
		
		Integer count = 0;
		for(Filter filter : filters) {
			if(filter.equals(filter1)) {
				count++;
			}
		}
		
		assertTrue(count == 1);
	}
	
	@Test
	public void parseQuery_MultipleLocales_MultipleTasks() {
		Filter filter1 = new Filter(FilterTypes.Locale, "locale1");
		Filter filter2 = new Filter(FilterTypes.Locale, "locale2");
		
		Set<Filter> filters = qs.parseQuery("(locale1) (locale2)");
		
		Integer dupCount1 = 0;
		Integer dupCount2 = 0;
		
		for(Filter filter : filters) {
			if(filter.equals(filter1)) {
				dupCount1++;
			}else if(filter.equals(filter2)) {
				dupCount2++;
			}
		}
		
		assertTrue(dupCount2 == 1);
		assertTrue(dupCount1 == 1);
	}
	
	@Test
	public void parseQuery_DefAndTerm_TwoFilters() {
		Set<Filter> filters = qs.parseQuery("a \"a\"");
		
		assertTrue(filters.size() == 2);
		
		for(Filter filter : filters) {
			assertTrue((filter.getType() == FilterTypes.Definition) || (filter.getType() == FilterTypes.Term));
		}
	}
	
	@Test
	public void parseQuery_DuplicateLocales_NoDuplicateTasks() {
		Filter filter1 = new Filter(FilterTypes.Locale, "locale1");
		
		Set<Filter> filters = qs.parseQuery("(locale1) (locale1)");
		
		Integer dupCount = 0;
		for(Filter filter : filters) {
			if(filter.equals(filter1)) {
				dupCount++;
			}
		}
		
		assertTrue(dupCount == 1);
	}
	
	@Test
	public void parseQuery_MultipleDuplicateLocales_NoDuplicateTasks() {
		Filter filter1 = new Filter(FilterTypes.Locale, "locale1");
		Filter filter2 = new Filter(FilterTypes.Locale, "locale3");
		
		Set<Filter> filters = qs.parseQuery("(locale1) (locale2) (locale1) (locale3) (locale4) (locale3)");
		
		Integer dupCount1 = 0;
		Integer dupCount2 = 0;
		
		for(Filter filter : filters) {
			if(filter.equals(filter1)) {
				dupCount1++;
			}else if(filter.equals(filter2)) {
				dupCount2++;
			}
		}
		
		assertTrue(dupCount1 == 1);
		assertTrue(dupCount2 == 1);
	}
	
	@Test
	public void parseQuery_MultipleFilterTypes_StandardOperation() {
		Set<Filter> filters = qs.parseQuery("(en-au) dama da dam");
		
		assertTrue(filters.size() == 4);
	}
	
	@Test
	public void getMaxResults_singleDefFilter_allResultsIncludeFilter() {
		Set<Filter> filters = qs.parseQuery("\"as\"");
		
		Set<Definition> defs = qs.getMaximumResultSet(filters);
		
		for(Definition def : defs) {
			assertTrue(def.getDefinition().contains("as"));
		}
	}
	
	@Test
	public void getMaxResults_twoDefFilters_resultsIncludeEitherFilter() {
		Set<Filter> filters = qs.parseQuery("\"as\" \"the\"");
		
		Set<Definition> defs = qs.getMaximumResultSet(filters);
		
		for(Definition def : defs) {
			assertTrue(def.getDefinition().contains("as") || 
					   def.getDefinition().contains("the"));
		}
	}
	
	@Test
	public void filterResults_TwoDefFilters_resultsIncludeBothFilters() {
		Set<Filter> filters = qs.parseQuery("\"as\" \"of\"");
		
		Set<Definition> defs = qs.getMaximumResultSet(filters);
		
		List<CompleteDefinition> completeDefs = qs.getCompleteDefinitions(defs);
		
		defs = qs.filterResults(completeDefs, filters);
		
		for(Definition def : defs) {
			assertTrue(def.getDefinition().contains("as"));
			assertTrue(def.getDefinition().contains("of"));
		}
	}
	
	
}
 