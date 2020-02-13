package com.firm58.rating.persistence.impl.jdbc;

import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_ACCOUNT;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_ACCOUNT_GROUP;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_ACCOUNT_TYPE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_AMOUNT;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_AMOUNT_BY_RATE_TYPE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_ASSET;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_AVERAGE_AMOUNT;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_AVERAGE_AMOUNT_BY_RATE_TYPE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_NODE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_NODE_DOMAIN;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_NODE_LONG_DESC;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_NODE_SHORT_DESC;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_PARTY_NAME;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_PARTY_ROUTE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_PARTY_TYPE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_PERIOD_DAY;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_PERIOD_FROM;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_PERIOD_MONTH;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_PERIOD_TO;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_RATED_VOLUME;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_RATED_VOLUME_BY_RATE_TYPE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_RATE_TYPE_GROUP;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_RATE_TYPE_GROUP_NAME;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_RATE_TYPE_SHORT_NAME;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_TRADE_TYPE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_TRANSACTION_TYPE;
import static com.firm58.rating.model.dbmap.RatingReportDBMap.FIELD_VOLUME;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.firm58.assetlibrary.common.service.AssetLibraryPublicService;
import com.firm58.common.core.exception.Firm58DatabaseException;
import com.firm58.common.core.exception.Firm58ExceptionTypeEnum;
import com.firm58.common.core.exception.Firm58RatingReportExceededSizeLimitException;
import com.firm58.common.model.DomainValue;
import com.firm58.common.model.rating.RatingReportRow;
import com.firm58.common.persistence.core.DomainValueDAO;
import com.firm58.rating.model.dbmap.RatingReportDBMap;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.sql.DataSource;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JdbcRatingReportDAOGetReportContentTest extends JdbcRatingReportDAOTestBase {

  private static final double CURRENCY_VAL = 100.0D;
  private static final String TEST_VALUE = "test";
  private static final String FILE_NAME = "/rating_report_" + System.currentTimeMillis() + ".csv";
  private static final String RAW_CSV_PRINTER_CLASS = JdbcRatingReportDAO.class.getName() + "$RawCSVPrinter";
  private static final String NODE_AMOUNT_OR_RATED_VOLUME_CLASS = RAW_CSV_PRINTER_CLASS + "$NodeAmountOrRatedVolume";
  private static final String NODE_VOLUME_CLASS = RAW_CSV_PRINTER_CLASS + "$NodeVolume";
  private static final String AMOUNT_TYPE = "|Amount";
  private static final String RATED_VOLUME_TYPE = "|Rated|Volume";
  private static final String AVG_AMOUNT_TYPE = "|Average|Amount";
  private static final String COL_NAME_CURRENCIES = "CURRENCIES";
  private static final String SET_PARENT_NODE_MTHD = "setParentNode";
  private static final String NODE_AMOUNT = "nodeAmount";
  private static final String PARENT_NODE = "PARENT";
  private static final String PARTY_TYPE = "PARTY-TYPE";
  private static final String PARTY_NAME = "PARTY-NAME";
  private static final String ROUTE = "ROUTE";
  private static final String DATE_ERROR = "Search requires a valid start and end date";

  private final ErrorCollector collector = new ErrorCollector();
  private final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public final TestRule rule = RuleChain.outerRule(collector).around(temporaryFolder);

  @Mock
  private DataSource dataSource;
  @Mock
  private Connection connection;
  @Mock
  private PreparedStatement preparedStatement;
  @Mock
  private ResultSet resultSet;
  @Mock
  private DomainValueDAO domainValueDAO;
  @Mock
  private AssetLibraryPublicService assetLibraryPublicService;

  @Captor
  private ArgumentCaptor<String> strArgCaptor;
  @Captor
  private ArgumentCaptor<Integer> intArgCaptor;
  @Captor
  private ArgumentCaptor<Object> objArgCaptor;
  private FileWriter writer;
  private JdbcRatingReportDAO jdbcRatingReportDAO;
  private Map<RatingReportDBMap, String> volumeColumnsMap;
  private List<String> stringValues;
  private List<Long> dateValues;
  private int numCols;

  @Before
  public void setup() throws Exception {
    jdbcRatingReportDAO = new JdbcRatingReportDAO();
    final String fileName = temporaryFolder.newFile(FILE_NAME).getAbsolutePath();
    writer = new FileWriter(fileName);
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString(anyInt())).thenReturn(STRING_FIELD_VAL);
    when(resultSet.getBigDecimal(anyInt())).thenReturn(BIG_DECIMAL_FIELD_VAL);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(dataSource.getConnection()).thenReturn(connection);
    jdbcRatingReportDAO.setDataSource(dataSource);
    jdbcRatingReportDAO.setFasterEnabled(true);
    jdbcRatingReportDAO.setAssetLibraryService(assetLibraryPublicService);
    final DomainValue domainValue = new DomainValue();
    domainValue.setDisplayName(STRING_FIELD_VAL);
    when(domainValueDAO.getById(anyString())).thenReturn(domainValue);
    jdbcRatingReportDAO.setDomainValueDAO(domainValueDAO);
    stringValues = new ArrayList<>(asList(STRING_FIELD_VAL, TXN_TYPE_1, TXN_TYPE_2));
    dateValues = asList(new Date(parseDate(DATE_FIELD_VAL).getTime()).getTime(),
        new Date(parseDate(NEXT_DATE_FIELD_VAL).getTime()).getTime());
    numCols = NUM_COLUMNS;
  }

  @After
  public void tearDown() throws Exception {
    writer.close();
  }

  @Test
  public void givenTableNotFoundForFieldAccountTypeWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT};

    try {
      // Act
      jdbcRatingReportDAO.getReportContent(getFilterByConditions(), filterByColumns,
          getPivotColumnsWith(FIELD_ACCOUNT_TYPE), getValueColumns(), RESULT_SIZE_LIMIT);
      fail();
    } catch (final Firm58DatabaseException e) {
      // Assert
      collector.checkThat(e.getMessage(), is(TABLE_NOT_FOUND_ERROR));
      collector.checkThat(e.getFirm58ExceptionType(), is(Firm58ExceptionTypeEnum.DATABASE_ERROR));
      verify(connection, never()).prepareStatement(anyString());
      checkStmt(0, 0, 0);
      checkResultSet(0, 0, 0);
      checkCloseResources(0, 0);
    }
  }

  @Test
  public void givenMoreResultsWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    when(resultSet.next()).thenReturn(true, true, false);
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT};

    try {
      // Act
      jdbcRatingReportDAO.getReportContent(getFilterByConditions(), filterByColumns,
          getPivotColumnsWith(FIELD_TRADE_TYPE), getValueColumns(), 1);
      fail();
    } catch (final Firm58RatingReportExceededSizeLimitException e) {
      // Assert
      collector.checkThat(e.getMessage(), is("Rating report exceeded the size limit of 1"));
      collector.checkThat(e.getFirm58ExceptionType(), nullValue(Firm58ExceptionTypeEnum.class));
      checkQuery(5);
      checkStmt(1, 9, 4);
      checkResultSet(6, 2, 2);
      checkCloseResources(1, 1);
    }
  }

  @Test
  public void givenInvalidPrepareStatementWhenGetReportContentAsRawCSVThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_PARTY_TYPE};
    when(connection.prepareStatement(anyString())).thenThrow(SQLException.class);

    try {
      // Act
      jdbcRatingReportDAO.getReportContent(getFilterByConditions(), filterByColumns,
          getPivotColumnsWith(FIELD_TRADE_TYPE), getValueColumns(), RESULT_SIZE_LIMIT);
      fail();
    } catch (final Firm58DatabaseException e) {
      collector.checkThat(e.getCause(), instanceOf(SQLException.class));
      collector.checkThat(e.getMessage(), is(format("%s\nfilter Conditions: {FIELD_TRADE_TYPE=%s, "
          + "FIELD_TRANSACTION_TYPE=%s, FIELD_PERIOD_FROM=%s, FIELD_PERIOD_TO=%s}\npivot columns: "
          + "[FIELD_NODE, FIELD_PERIOD_MONTH, FIELD_TRADE_TYPE]\nvalue columns: [FIELD_RATE_TYPE_SHORT_NAME, "
          + "FIELD_TRANSACTION_TYPE]", DATABASE_ERROR, TRADE_TYPES, TXN_TYPES, DATE_FIELD_VAL, DATE_FIELD_VAL)));
      collector.checkThat(e.getFirm58ExceptionType(), is(Firm58ExceptionTypeEnum.DATABASE_ERROR));
      checkQuery(4);
      checkStmt(0, 0, 0);
      checkResultSet(0, 0, 0);
      checkCloseResources(0, 1);
    }
  }

  @Test
  public void givenFilterColumnsNoFieldTradeTypeWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_TRADE_TYPE), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(3);
    checkStmt(1, 9, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenFilterColumnsNoFieldPeriodToWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_TRADE_TYPE), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(2);
    checkStmt(1, 9, 2);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenNoColumnsWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT};
    final List<RatingReportDBMap> valueColumns = getValueColumns();
    valueColumns.remove(FIELD_RATE_TYPE_SHORT_NAME);
    valueColumns.remove(FIELD_TRANSACTION_TYPE);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(emptyMap(), filterByColumns, emptyList(), valueColumns, RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(6);
    checkStmt(1, 0, 0);
    checkResultSet(0, 0, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldPartyTypeWhenGetReportContentAsRawCSVThenExpectedResults() throws Exception {
    // Arrange
    when(resultSet.getString(3)).thenReturn(FIELD_PARTY_TYPE_VAL.name());
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_PARTY_TYPE};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_PARTY_TYPE), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(14);
    checkStmt(1, 5, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldPeriodDayWhenGetReportContentAsRawCSVThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_PERIOD_DAY};
    final List<RatingReportDBMap> pivotColumns = getPivotColumnsWith(FIELD_PERIOD_DAY);
    volumeColumnsMap.remove(FIELD_PERIOD_MONTH);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            pivotColumns, getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(15);
    checkStmt(1, 4, 4);
    checkResultSet(2, 2, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldAccountWhenGetReportContentAsRawCSVThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_ACCOUNT};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_ACCOUNT), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(9);
    checkStmt(1, 5, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldAssetWhenGetReportContentAsRawCSVThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_ASSET};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_ASSET), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(10);
    checkStmt(1, 5, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldPartyNameWhenGetReportContentAsRawCSVThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_PARTY_NAME};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_PARTY_NAME), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(11);
    checkStmt(1, 5, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldPartyRouteWhenGetReportContentAsRawCSVThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_PARTY_ROUTE};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_PARTY_ROUTE), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(12);
    checkStmt(1, 5, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldPartyTradeTypeWhenGetReportContentAsRawCSVThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_TRADE_TYPE};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_TRADE_TYPE), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(13);
    checkStmt(1, 3, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenValueColumnsNoFieldVolumeWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT};
    final List<RatingReportDBMap> valueColumns = getValueColumns();
    valueColumns.remove(FIELD_VOLUME);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_TRADE_TYPE), valueColumns, RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(23);
    checkStmt(1, 5, 2);
    checkResultSet(3, 1, 0);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsNoFieldTradeTypeWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns =
        {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT, FIELD_TRADE_TYPE};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumns(), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(20);
    checkStmt(1, 5, 4);
    checkResultSet(2, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenFilterByColumnsNoFieldNodeWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE_DOMAIN,
        FIELD_ACCOUNT_GROUP, FIELD_AMOUNT, FIELD_TRADE_TYPE};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumns(), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(0);
    checkStmt(1, 5, 4);
    checkResultSet(2, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenFilterByColumnsNoFieldTradeTypeWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns =
        {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_TRADE_TYPE), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(1);
    checkStmt(1, 5, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenNoFieldTradeTypeWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns =
        {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_NODE), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(8);
    checkStmt(1, 7, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenNoFieldNodeAndNoFieldDomainTypeWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns =
        {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT, FIELD_PARTY_TYPE};

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumns(), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(7);
    checkStmt(1, 5, 4);
    checkResultSet(2, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsNodeShortDescWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT, FIELD_TRANSACTION_TYPE};
    final List<RatingReportDBMap> pivotColumns = getPivotColumnsWith(FIELD_NODE_SHORT_DESC);
    pivotColumns.remove(FIELD_NODE);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns, pivotColumns, getValueColumns(),
            RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(19);
    checkStmt(1, 11, 4);
    checkResultSet(2, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsNodeLongDescWhenGetReportContentThenExpectedResults() throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT, FIELD_TRANSACTION_TYPE};
    final List<RatingReportDBMap> pivotColumns = getPivotColumnsWith(FIELD_NODE_LONG_DESC);
    pivotColumns.remove(FIELD_NODE);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns, pivotColumns, getValueColumns(),
            RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(18);
    checkStmt(1, 11, 4);
    checkResultSet(2, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsNoNodeFilterOnNodeFirstWhenGetReportContentThenExpectedResults()
      throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE, FIELD_NODE_DOMAIN,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT, FIELD_RATE_TYPE_GROUP};
    final List<RatingReportDBMap> pivotColumns = getPivotColumnsWith(FIELD_TRADE_TYPE);
    pivotColumns.remove(FIELD_NODE);
    volumeColumnsMap.remove(FIELD_NODE);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns, pivotColumns, getValueColumns(),
            RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(22);
    checkStmt(1, 10, 4);
    checkResultSet(2, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsNoNodeFilterOnNodeDomainFirstWhenGetReportContentThenExpectedResults()
      throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_NODE_DOMAIN, FIELD_NODE,
        FIELD_NODE_DOMAIN, FIELD_ACCOUNT_GROUP, FIELD_AMOUNT, FIELD_RATE_TYPE_GROUP};
    final List<RatingReportDBMap> pivotColumns = getPivotColumnsWith(FIELD_TRADE_TYPE);
    pivotColumns.remove(FIELD_NODE);
    volumeColumnsMap.remove(FIELD_NODE);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns, pivotColumns, getValueColumns(),
            RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(21);
    checkStmt(1, 10, 4);
    checkResultSet(2, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldRateTypeGroupTypeWhenGetReportContentAsRawCSVThenExpectedResults()
      throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_PARTY_TYPE};
    final List<RatingReportDBMap> valueColumns = getValueColumns();
    valueColumns.remove(FIELD_VOLUME);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_RATE_TYPE_GROUP), valueColumns, RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(17);
    checkStmt(1, 3, 2);
    checkResultSet(3, 1, 0);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenPivotColumnsFieldRateTypeGroupNameWhenGetReportContentAsRawCSVThenExpectedResults()
      throws Exception {
    // Arrange
    final RatingReportDBMap[] filterByColumns = {FIELD_PERIOD_TO, FIELD_PERIOD_FROM, FIELD_ACCOUNT_GROUP,
        FIELD_AMOUNT, FIELD_PARTY_TYPE};
    final List<RatingReportDBMap> valueColumns = getValueColumns();
    valueColumns.remove(FIELD_VOLUME);

    // Act
    final List<RatingReportRow> result = jdbcRatingReportDAO
        .getReportContent(getFilterByConditions(), filterByColumns,
            getPivotColumnsWith(FIELD_RATE_TYPE_GROUP_NAME), getValueColumns(), RESULT_SIZE_LIMIT);

    // Assert
    checkRow(result);
    checkQuery(16);
    checkStmt(1, 5, 4);
    checkResultSet(3, 1, 1);
    checkCloseResources(1, 1);
  }

  @Test
  public void givenAllParametersSetThenWhenPrintGrandTotalRowThenReturnGrandTotalAmount() throws Exception {
    // Arrange
    final String query = setUpQuery();
    final Map<String, Set<String>> columnNames = setUpColumnNames();
    final Object rawCSVPrinter = getRawCSVPrinter(query, columnNames);
    final Method printGrandTotalRow = rawCSVPrinter.getClass().getMethod("printGrandTotalRow");
    printGrandTotalRow.invoke(rawCSVPrinter);

    // Act
    printGrandTotalRow.invoke(rawCSVPrinter);

    // Assert
    final double amount = CURRENCY_VAL + CURRENCY_VAL;
    final Class<?> innerClazz = rawCSVPrinter.getClass();
    final Field grandTotalAmountField = innerClazz.getDeclaredField("grandTotalAmount");
    grandTotalAmountField.setAccessible(true);
    final Map<String, Object> grandTotalAmountMap = (Map<String, Object>) grandTotalAmountField.get(rawCSVPrinter);
    final Field grandTotalRatedVolumeField = innerClazz.getDeclaredField("grandTotalRatedVolume");
    final Map<String, Object> grandTotalRatedVolumeMap = (Map<String, Object>) grandTotalRatedVolumeField
        .get(rawCSVPrinter);
    collector.checkThat(1, is(grandTotalAmountMap.size()));
    collector.checkThat(true, is(grandTotalAmountMap.containsKey(AMOUNT_TYPE)));
    final Map<String, Object> amountMap = (Map<String, Object>) grandTotalAmountMap.get(AMOUNT_TYPE);
    collector.checkThat(true, is(amountMap.containsKey(FIELD_CURRENCY_VAL)));
    collector.checkThat(amount, is(amountMap.get(FIELD_CURRENCY_VAL)));
    collector.checkThat(1, is(grandTotalRatedVolumeMap.size()));
    collector.checkThat(true, is(grandTotalRatedVolumeMap.containsKey(RATED_VOLUME_TYPE)));
    final Map<String, Object> ratedVolumeMap = (Map<String, Object>) grandTotalAmountMap.get(AMOUNT_TYPE);
    collector.checkThat(amount, is(ratedVolumeMap.get(FIELD_CURRENCY_VAL)));
    final Field lastRowDataField = innerClazz.getDeclaredField("lastRowData");
    lastRowDataField.setAccessible(true);
    final RatingReportRowData lastRowData = (RatingReportRowData) lastRowDataField.get(rawCSVPrinter);
    collector.checkThat(lastRowData.getAmountData().size(), is(2));
    collector.checkThat(lastRowData.getAmountData().get(AMOUNT_TYPE).getAmountInfo(FIELD_CURRENCY_VAL).getAmount(),
        is(amount));
    collector.checkThat(
        lastRowData.getAmountData().get(AMOUNT_TYPE).getAmountInfo(FIELD_CURRENCY_VAL).getCurrencyShortName(),
        is(FIELD_CURRENCY_VAL));
    collector.checkThat(
        lastRowData.getAmountData().get(FIELD_AMOUNT.name()).getAmountInfo(FIELD_CURRENCY_VAL).getAmount(),
        is(amount));
    collector.checkThat(lastRowData.getAmountData().get(FIELD_AMOUNT.name()).getAmountInfo(FIELD_CURRENCY_VAL)
        .getCurrencyShortName(), is(FIELD_CURRENCY_VAL));
    collector.checkThat(lastRowData.getRatedVolumeData().size(), is(2));
    collector.checkThat(
        lastRowData.getRatedVolumeData().get(RATED_VOLUME_TYPE).getAmountInfo(FIELD_CURRENCY_VAL).getAmount(),
        is(amount));
    collector.checkThat(lastRowData.getRatedVolumeData().get(RATED_VOLUME_TYPE).getAmountInfo(FIELD_CURRENCY_VAL)
        .getCurrencyShortName(), is(FIELD_CURRENCY_VAL));
    collector.checkThat(
        lastRowData.getRatedVolumeData().get(FIELD_RATED_VOLUME.name()).getAmountInfo(FIELD_CURRENCY_VAL)
            .getAmount(), is(amount));
    collector.checkThat(
        lastRowData.getRatedVolumeData().get(FIELD_RATED_VOLUME.name()).getAmountInfo(FIELD_CURRENCY_VAL)
            .getCurrencyShortName(), is(FIELD_CURRENCY_VAL));
    collector.checkThat(lastRowData.getAverageAmountData().size(), is(2));
    collector.checkThat(
        lastRowData.getAverageAmountData().get(AVG_AMOUNT_TYPE).getAmountInfo(FIELD_CURRENCY_VAL).getAmount(),
        is(1.0D));
    collector.checkThat(lastRowData.getAverageAmountData().get(AVG_AMOUNT_TYPE).getAmountInfo(FIELD_CURRENCY_VAL)
        .getCurrencyShortName(), is(FIELD_CURRENCY_VAL));
    collector.checkThat(
        lastRowData.getAverageAmountData().get(FIELD_AVERAGE_AMOUNT.name()).getAmountInfo(FIELD_CURRENCY_VAL)
            .getAmount(), is(Double.POSITIVE_INFINITY));
    collector.checkThat(
        lastRowData.getAverageAmountData().get(FIELD_AVERAGE_AMOUNT.name()).getAmountInfo(FIELD_CURRENCY_VAL)
            .getCurrencyShortName(), is(FIELD_CURRENCY_VAL));
  }

  @Test
  public void givenStartDateNullWhenGetTotalVolumeThenExpectedResults() throws Exception {
    try {
      // Act
      jdbcRatingReportDAO
          .getTotalVolume(null, parseDate(NEXT_DATE_FIELD_VAL), PARTY_TYPE, PARTY_NAME, ROUTE);
      fail();
    } catch (final Firm58DatabaseException e) {
      // Assert
      collector.checkThat(e.getMessage(), is(DATE_ERROR));
      collector.checkThat(e.getFirm58ExceptionType(), is(Firm58ExceptionTypeEnum.DATABASE_ERROR));
      verify(connection, never()).prepareStatement(anyString());
      checkStmt(0, 0, 0);
      checkResultSet(0, 0, 0);
      checkCloseResources(0, 0);
    }
  }

  @Test
  public void givenEndDateNullWhenGetTotalVolumeThenExpectedResults() throws Exception {
    try {
      // Act
      jdbcRatingReportDAO
          .getTotalVolume(parseDate(DATE_FIELD_VAL), null, PARTY_TYPE, PARTY_NAME, ROUTE);
      fail();
    } catch (final Firm58DatabaseException e) {
      // Assert
      collector.checkThat(e.getMessage(), is(DATE_ERROR));
      collector.checkThat(e.getFirm58ExceptionType(), is(Firm58ExceptionTypeEnum.DATABASE_ERROR));
      verify(connection, never()).prepareStatement(anyString());
      checkStmt(0, 0, 0);
      checkResultSet(0, 0, 0);
      checkCloseResources(0, 0);
    }
  }

  @Test
  public void givenPartyTypeEmptyWhenGetTotalVolumeThenExpectedResults() throws Exception {
    try {
      // Act
      jdbcRatingReportDAO
          .getTotalVolume(parseDate(DATE_FIELD_VAL), parseDate(NEXT_DATE_FIELD_VAL), "", PARTY_NAME, ROUTE);
      fail();
    } catch (final Firm58DatabaseException e) {
      // Assert
      collector.checkThat(e.getMessage(), is("Party Type is required"));
      collector.checkThat(e.getFirm58ExceptionType(), is(Firm58ExceptionTypeEnum.DATABASE_ERROR));
      verify(connection, never()).prepareStatement(anyString());
      checkStmt(0, 0, 0);
      checkResultSet(0, 0, 0);
      checkCloseResources(0, 0);
    }
  }

  @Test
  public void givenResultSetExcpWhenGetTotalVolumeThenExpectedResults() throws Exception {
    // Arrange
    numCols = 5;
    stringValues.addAll(asList(PARTY_TYPE, PARTY_NAME, ROUTE));
    when(resultSet.getBigDecimal(1)).thenThrow(SQLException.class);

    try {
      // Act
      jdbcRatingReportDAO
          .getTotalVolume(parseDate(DATE_FIELD_VAL), parseDate(NEXT_DATE_FIELD_VAL), PARTY_TYPE, PARTY_NAME,
              ROUTE);
      fail();
    } catch (final Firm58DatabaseException e) {
      // Assert
      collector.checkThat(e.getCause(), instanceOf(SQLException.class));
      collector.checkThat(e.getMessage(), is(DATABASE_ERROR));
      collector.checkThat(e.getFirm58ExceptionType(), is(Firm58ExceptionTypeEnum.DATABASE_ERROR));
      checkQuery(24);
      checkStmt(1, 3, 2);
      checkResultSet(0, 0, 1);
      checkCloseResources(1, 1);
    }
  }

  @Test
  public void givenAllParametersWhenGetTotalVolumeThenExpectedResults() throws Exception {
    // Arrange
    numCols = 5;
    stringValues.addAll(asList(PARTY_TYPE, PARTY_NAME, ROUTE));

    // Act
    final BigDecimal result = jdbcRatingReportDAO
        .getTotalVolume(parseDate(DATE_FIELD_VAL), parseDate(NEXT_DATE_FIELD_VAL), PARTY_TYPE, PARTY_NAME, ROUTE);

    // Assert
    collector.checkThat(result, is(BIG_DECIMAL_FIELD_VAL));
    checkQuery(24);
    checkStmt(1, 3, 2);
    checkResultSet(0, 0, 1);
    checkCloseResources(1, 1);
  }

  private void checkCloseResources(final int timesCloseRes, final int timesCloseConn) throws SQLException {
    verify(resultSet, times(timesCloseRes)).close();
    verify(preparedStatement, times(timesCloseRes)).close();
    verify(connection, times(timesCloseConn)).close();
  }

  private void checkQuery(final int lineNum) throws SQLException, IOException {
    verify(connection).prepareStatement(strArgCaptor.capture());
    final List<String> lines = readLines();
    final String line = lines.get(lineNum);
    final String query = strArgCaptor.getValue().trim();
    if (line.contains(VOLUME_COLUMNS_MARKER)) {
      /* workaround approach as the column order is differing in the query randomly */
      final String[] lineParts = line.split(VOLUME_COLUMNS_MARKER);
      collector.checkThat(query, startsWith(lineParts[0]));
      collector.checkThat(query, endsWith(lineParts[1]));
      final int startIdx = lineParts[0].length();
      final List<String> queryParts = asList(
          query.substring(startIdx, query.indexOf(lineParts[1], startIdx)).split(VOLUME_INFO_FIELD_DELIM));
      for (final Map.Entry<RatingReportDBMap, String> entry : volumeColumnsMap.entrySet()) {
        final RatingReportDBMap key = entry.getKey();
        final String value = entry.getValue();
        if (FIELD_PARTY_TYPE == key
            || FIELD_PARTY_NAME == key
            || FIELD_PARTY_ROUTE == key) {
          collector.checkThat(queryParts.contains(
              format(VOLUME_INFO_NVL_FIELD, key.getDBName(), value)), is(true));
        } else {
          collector.checkThat(queryParts.contains(
              format(VOLUME_INFO_FIELD, key.getDBName(), value)), is(true));
        }
      }
    } else {
      collector.checkThat(query, is(line));
    }
  }

  private void checkStmt(final int timesExecQuery, final int timesSetString,
      final int timesSetDate) throws SQLException {
    verify(preparedStatement, times(timesExecQuery)).executeQuery();
    verify(preparedStatement, times(timesSetString))
        .setString(intArgCaptor.capture(), (String) objArgCaptor.capture());
    verify(preparedStatement, times(timesSetDate)).setDate(intArgCaptor.capture(), (Date) objArgCaptor.capture());
    final List<Integer> paramIndexes = intArgCaptor.getAllValues();
    final List<Object> values = objArgCaptor.getAllValues();
    if (0 < timesSetString + timesSetDate) {
      collector.checkThat(paramIndexes, hasItems(greaterThan(0)));
      collector.checkThat(paramIndexes, hasItems(lessThan(numCols + 1)));
      for (final Object value : values) {
        if (value instanceof String) {
          collector.checkThat(stringValues.contains(value), is(true));
        } else if (value instanceof Date) {
          collector.checkThat(dateValues.contains(((Date) value).getTime()), is(true));
        }
      }
    }
    verify(preparedStatement, never()).setBigDecimal(anyInt(), any(BigDecimal.class));
  }

  private void checkResultSet(final int timesGetString, final int timesGetDate,
      final int timesGetBigDecimal) throws SQLException {
    verify(resultSet, times(timesGetString)).getString(intArgCaptor.capture());
    verify(resultSet, times(timesGetDate)).getDate(intArgCaptor.capture());
    verify(resultSet, times(timesGetBigDecimal)).getBigDecimal(intArgCaptor.capture());
    final List<Integer> paramIndexes = intArgCaptor.getAllValues();
    if (0 < timesGetString + timesGetDate + timesGetBigDecimal) {
      collector.checkThat(paramIndexes, hasItems(greaterThan(0)));
      collector.checkThat(paramIndexes, hasItems(lessThan(numCols + 1)));
    }
  }

  private void checkRow(final List<RatingReportRow> result) {
    collector.checkThat(1, is(result.size()));
    collector.checkThat(BigDecimal.ZERO, is(result.get(0).getAmount()));
    collector.checkThat(BigDecimal.ONE, is(result.get(0).getRatedVolume()));
  }

  private Object getRawCSVPrinter(final String query, final Map<String, Set<String>> columnNames)
      throws ReflectiveOperationException {
    final Class<?> innerClazz = Class.forName(RAW_CSV_PRINTER_CLASS);
    final Constructor<?> constructor = innerClazz.getDeclaredConstructor(String.class, TreeMap.class, Writer.class);
    constructor.setAccessible(true);
    final Object rawCSVPrinter = constructor.newInstance(query, columnNames, writer);
    setUpNodes(rawCSVPrinter);
    return rawCSVPrinter;
  }

  private List<RatingReportDBMap> getPivotColumnsWith(final RatingReportDBMap ratingReportDBMap) {
    volumeColumnsMap = new HashMap<>();
    volumeColumnsMap.put(FIELD_NODE, VOLUME_NODE_ID);
    volumeColumnsMap.put(FIELD_PERIOD_MONTH, VOLUME_MONTH);
    final List<RatingReportDBMap> pivotColumns = new ArrayList<>(asList(FIELD_NODE, FIELD_PERIOD_MONTH));
    if (null != ratingReportDBMap) {
      if (FIELD_PERIOD_DAY == ratingReportDBMap ||
          FIELD_ACCOUNT == ratingReportDBMap ||
          FIELD_ASSET == ratingReportDBMap ||
          FIELD_PARTY_NAME == ratingReportDBMap ||
          FIELD_PARTY_ROUTE == ratingReportDBMap ||
          FIELD_PARTY_TYPE == ratingReportDBMap ||
          FIELD_TRADE_TYPE == ratingReportDBMap) {
        volumeColumnsMap.put(ratingReportDBMap, VOLUME_PREFIX + ratingReportDBMap.getDBName());
      }
      pivotColumns.add(ratingReportDBMap);
    }
    return pivotColumns;
  }

  private List<RatingReportDBMap> getPivotColumns() {
    return getPivotColumnsWith(null);
  }

  private static void setUpNodes(final Object rawCSVPrinter) throws ReflectiveOperationException {
    final Class<?> innerClazz = rawCSVPrinter.getClass();
    final Field nodeVolumeField = innerClazz.getDeclaredField("nodeVolume");
    final Map<String, Object> nodeVolumeMap = (Map<String, Object>) nodeVolumeField.get(rawCSVPrinter);
    nodeVolumeMap.put("nodeVolume1", getNodeVolume());
    final Field nodeRatedVolumeField = innerClazz.getDeclaredField("nodeRatedVolume");
    final Map<String, Object> nodeRatedVolumeMap = (Map<String, Object>) nodeRatedVolumeField.get(rawCSVPrinter);
    nodeRatedVolumeMap.put("nodeRatedVolume1", getNodeRatedVolume());
    final Field nodeAmountField = innerClazz.getDeclaredField(NODE_AMOUNT);
    final Map<String, Object> nodeAmountMap = (Map<String, Object>) nodeAmountField.get(rawCSVPrinter);
    nodeAmountMap.put(NODE_AMOUNT, getNodeAmount());
  }

  private static Map<String, Set<String>> setUpColumnNames() {
    final Map<String, Set<String>> columnNames = new TreeMap<>();
    columnNames.put(FIELD_AMOUNT.name(), new TreeSet<>());
    columnNames.put(FIELD_RATED_VOLUME.name(), new TreeSet<>());
    columnNames.put(FIELD_AVERAGE_AMOUNT.name(), new TreeSet<>());
    columnNames.put(FIELD_AVERAGE_AMOUNT_BY_RATE_TYPE.name(), new TreeSet<>());
    columnNames.put(COL_NAME_CURRENCIES, new TreeSet<>());
    columnNames.get(FIELD_AMOUNT.name()).add("100");
    columnNames.get(FIELD_AVERAGE_AMOUNT.name()).add("60");
    columnNames.get(FIELD_AVERAGE_AMOUNT_BY_RATE_TYPE.name()).add("60");
    columnNames.get(FIELD_RATED_VOLUME.name()).add("10");
    columnNames.get(COL_NAME_CURRENCIES).add(FIELD_CURRENCY_VAL);
    return columnNames;
  }

  private static String setUpQuery() {
    final StringBuilder query = new StringBuilder("pivot=").append(FIELD_NODE.name()).append(",")
        .append(FIELD_NODE_SHORT_DESC.name()).append(",")
        .append(FIELD_NODE_LONG_DESC.name()).append("|").append("value=")
        .append(FIELD_AMOUNT_BY_RATE_TYPE.name()).append(",")
        .append(FIELD_RATED_VOLUME_BY_RATE_TYPE.name()).append(",")
        .append(FIELD_AMOUNT.name()).append(",")
        .append(FIELD_AVERAGE_AMOUNT.name()).append(",")
        .append(FIELD_RATED_VOLUME.name()).append(",")
        .append(FIELD_AVERAGE_AMOUNT_BY_RATE_TYPE.name()).append(",")
        .append(FIELD_VOLUME.name()).append(",").append("|").append("amount=");
    return query.toString();
  }

  private static Object getNodeVolume() throws ReflectiveOperationException {
    final Class<?> innerClazz = Class.forName(NODE_VOLUME_CLASS);
    final Constructor<?> constructor = innerClazz.getDeclaredConstructor(String.class);
    final Method setParentNodeMethod = innerClazz.getDeclaredMethod(SET_PARENT_NODE_MTHD, String.class);
    final Object nodeVolume = constructor.newInstance(TEST_VALUE);
    setParentNodeMethod.invoke(nodeVolume, PARENT_NODE);
    return nodeVolume;
  }

  private static Object getNodeAmount() throws ReflectiveOperationException {
    return getNodeAmountOrRatedVolume(AMOUNT_TYPE);
  }

  private static Object getNodeRatedVolume() throws ReflectiveOperationException {
    return getNodeAmountOrRatedVolume(RATED_VOLUME_TYPE);
  }

  private static Object getNodeAmountOrRatedVolume(final String amountInfoType) throws ReflectiveOperationException {
    final Class<?> innerClazz = Class.forName(NODE_AMOUNT_OR_RATED_VOLUME_CLASS);
    final Constructor<?> constructor = innerClazz.getDeclaredConstructor(String.class);
    final Object nodeAmountOrRatedVolume = constructor.newInstance(TEST_VALUE);
    final Method addToTotalMethod = innerClazz.getDeclaredMethod("addToTotal", Map.class);
    final Method setParentNodeMethod = innerClazz.getDeclaredMethod(SET_PARENT_NODE_MTHD, String.class);
    final Map<String, AmountInfoList> data = new HashMap<>();
    final AmountInfoList amountInfoList = new AmountInfoList();
    final AmountInfo amountInfo = new AmountInfo();
    amountInfo.setAmount(CURRENCY_VAL);
    amountInfo.setCurrencyShortName(FIELD_CURRENCY_VAL);
    amountInfo.setCurrencySymbol(FIELD_CURRENCY_VAL);
    amountInfoList.addAmount(amountInfo);
    data.put(amountInfoType, amountInfoList);
    addToTotalMethod.invoke(nodeAmountOrRatedVolume, data);
    setParentNodeMethod.invoke(nodeAmountOrRatedVolume, PARENT_NODE);
    return nodeAmountOrRatedVolume;
  }

  private static List<RatingReportDBMap> getValueColumns() {
    return new ArrayList<>(asList(FIELD_RATE_TYPE_SHORT_NAME, FIELD_TRANSACTION_TYPE, FIELD_VOLUME));
  }

  private static Map<String, String> getFilterByConditions() {
    final Map<String, String> filterByConditions = new HashMap<>();
    filterByConditions.put(FIELD_TRADE_TYPE.name(), TRADE_TYPES);
    filterByConditions.put(FIELD_PERIOD_TO.name(), DATE_FIELD_VAL);
    filterByConditions.put(FIELD_PERIOD_FROM.name(), DATE_FIELD_VAL);
    filterByConditions.put(FIELD_TRANSACTION_TYPE.name(), TXN_TYPES);
    return filterByConditions;
  }

  private static List<String> readLines() throws IOException {
    try (final InputStream inputStream = JdbcRatingReportDAOGetReportContentTest.class.getResourceAsStream(
        "JdbcRatingReportDAOReportQuery.txt")) {
      return IOUtils.readLines(inputStream, UTF_8);
    }
  }
}