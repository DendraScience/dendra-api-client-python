"""Unit tests for dendra_api_client.py"""

import unittest
from unittest.mock import patch, MagicMock
import datetime as dt
import pytz

import dendra_api_client as dendra


def _mock_response(data, status_code=200):
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = {'data': data}
    return mock_resp


class TestTimeHelpers(unittest.TestCase):

    def test_time_utc_empty_returns_utc_datetime(self):
        result = dendra.time_utc()
        self.assertIsInstance(result, dt.datetime)
        self.assertEqual(result.tzinfo, pytz.utc)

    def test_time_utc_parses_utc_string(self):
        result = dendra.time_utc('2019-03-01T08:00:00Z')
        self.assertEqual(result.year, 2019)
        self.assertEqual(result.month, 3)
        self.assertEqual(result.day, 1)
        self.assertEqual(result.hour, 8)

    def test_time_utc_converts_offset_to_utc(self):
        result = dendra.time_utc('2019-03-01T08:00:00-0400')
        self.assertEqual(result.hour, 12)  # 8 AM EDT = 12 PM UTC

    def test_time_format_default_is_local_style(self):
        result = dendra.time_format()
        self.assertRegex(result, r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$')
        self.assertFalse(result.endswith('Z'))

    def test_time_format_utc_appends_z(self):
        dt_time = dt.datetime(2019, 3, 1, 8, 0, 0)
        result = dendra.time_format(dt_time, 'utc')
        self.assertEqual(result, '2019-03-01T08:00:00Z')

    def test_time_format_local_no_z(self):
        dt_time = dt.datetime(2019, 3, 1, 8, 0, 0)
        result = dendra.time_format(dt_time, 'local')
        self.assertEqual(result, '2019-03-01T08:00:00')

    def test_time_format_with_explicit_datetime(self):
        dt_time = dt.datetime(2020, 6, 15, 12, 30, 45)
        result = dendra.time_format(dt_time)
        self.assertEqual(result, '2020-06-15T12:30:45')


class TestInternalHelpers(unittest.TestCase):

    def setUp(self):
        self._saved = dendra.headers.copy()

    def tearDown(self):
        dendra.headers.clear()
        dendra.headers.update(self._saved)

    def test_validate_mongo_id_valid(self):
        dendra._validate_mongo_id('5ae8793efe27f424f9102b87')  # no exception

    def test_validate_mongo_id_bad_type(self):
        with self.assertRaises(TypeError):
            dendra._validate_mongo_id(12345)

    def test_validate_mongo_id_too_short(self):
        with self.assertRaises(ValueError):
            dendra._validate_mongo_id('tooshort')

    def test_validate_mongo_id_too_long(self):
        with self.assertRaises(ValueError):
            dendra._validate_mongo_id('a' * 25)

    def test_check_auth_raises_without_token(self):
        dendra.headers.pop('Authorization', None)
        with self.assertRaises(RuntimeError):
            dendra._check_auth()

    def test_check_auth_passes_with_token(self):
        dendra.headers['Authorization'] = 'test_token'
        dendra._check_auth()  # should not raise


class TestListFunctions(unittest.TestCase):

    def setUp(self):
        self._saved = dendra.headers.copy()
        dendra.headers.pop('Authorization', None)

    def tearDown(self):
        dendra.headers.clear()
        dendra.headers.update(self._saved)

    @patch('dendra_api_client.requests.get')
    def test_get_organization_id(self, mock_get):
        mock_get.return_value = _mock_response([{'_id': 'abc123def456abc123def456'}])
        result = dendra.get_organization_id('erczo')
        self.assertEqual(result, 'abc123def456abc123def456')
        mock_get.assert_called_once()

    @patch('dendra_api_client.requests.get')
    def test_list_organizations_all(self, mock_get):
        data = [
            {'_id': 'abc123def456abc123def456', 'name': 'ERCZO', 'slug': 'erczo'},
            {'_id': 'def456abc123def456abc123', 'name': 'UCNRS', 'slug': 'ucnrs'},
        ]
        mock_get.return_value = _mock_response(data)
        result = dendra.list_organizations()
        self.assertEqual(len(result), 2)

    @patch('dendra_api_client.requests.get')
    def test_list_organizations_filtered(self, mock_get):
        data = [{'_id': 'abc123def456abc123def456', 'name': 'ERCZO', 'slug': 'erczo'}]
        mock_get.return_value = _mock_response(data)
        result = dendra.list_organizations('erczo')
        self.assertEqual(len(result), 1)
        self.assertEqual(mock_get.call_args[1]['params']['slug'], 'erczo')

    @patch('dendra_api_client.requests.get')
    def test_list_stations_all(self, mock_get):
        data = [{'_id': 'aaa111bbb222ccc333ddd444', 'name': 'Station A', 'slug': 'station-a'}]
        mock_get.return_value = _mock_response(data)
        result = dendra.list_stations()
        self.assertEqual(len(result), 1)

    @patch('dendra_api_client.requests.get')
    def test_list_stations_by_org(self, mock_get):
        org_data = [{'_id': 'abc123def456abc123def456', 'name': 'UCNRS', 'slug': 'ucnrs'}]
        stn_data = [{'_id': 'aaa111bbb222ccc333ddd444', 'name': 'Stunt Ranch', 'slug': 'stunt-ranch'}]
        mock_get.side_effect = [_mock_response(org_data), _mock_response(stn_data)]
        result = dendra.list_stations('ucnrs')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Stunt Ranch')

    @patch('dendra_api_client.requests.get')
    def test_list_stations_bad_org_returns_error(self, mock_get):
        mock_get.return_value = _mock_response([])
        result = dendra.list_stations('nonexistent')
        self.assertIn('ERROR', result)

    @patch('dendra_api_client.requests.get')
    def test_list_datastreams_by_station_id(self, mock_get):
        data = [{'_id': 'aaa111bbb222ccc333ddd444', 'name': 'Air Temp Avg'}]
        mock_get.return_value = _mock_response(data)
        result = dendra.list_datastreams_by_station_id('aaa111bbb222ccc333ddd444')
        self.assertEqual(len(result), 1)
        self.assertEqual(mock_get.call_args[1]['params']['station_id'], 'aaa111bbb222ccc333ddd444')

    @patch('dendra_api_client.requests.get')
    def test_list_datastreams_by_query(self, mock_get):
        data = [{'_id': 'aaa111bbb222ccc333ddd444', 'name': 'Air Temp Avg'}]
        mock_get.return_value = _mock_response(data)
        result = dendra.list_datastreams_by_query(station_id='aaa111bbb222ccc333ddd444')
        self.assertEqual(len(result), 1)

    @patch('dendra_api_client.requests.get')
    def test_needs_auth_blocks_without_token(self, mock_get):
        with self.assertRaises(RuntimeError):
            dendra.list_organizations(needs_auth=True)
        mock_get.assert_not_called()

    @patch('dendra_api_client.requests.get')
    def test_needs_auth_passes_with_token(self, mock_get):
        dendra.headers['Authorization'] = 'test_token'
        mock_get.return_value = _mock_response([])
        dendra.list_organizations(needs_auth=True)
        mock_get.assert_called_once()


class TestGetMeta(unittest.TestCase):

    def setUp(self):
        self._saved = dendra.headers.copy()

    def tearDown(self):
        dendra.headers.clear()
        dendra.headers.update(self._saved)

    def test_get_meta_station_bad_type_raises(self):
        with self.assertRaises(TypeError):
            dendra.get_meta_station_by_id(12345)

    def test_get_meta_station_bad_length_raises(self):
        with self.assertRaises(ValueError):
            dendra.get_meta_station_by_id('short')

    def test_get_meta_datastream_bad_type_raises(self):
        with self.assertRaises(TypeError):
            dendra.get_meta_datastream_by_id(12345)

    def test_get_meta_datastream_bad_length_raises(self):
        with self.assertRaises(ValueError):
            dendra.get_meta_datastream_by_id('short')

    def test_get_meta_annotation_bad_type_raises(self):
        with self.assertRaises(TypeError):
            dendra.get_meta_annotation(12345)

    @patch('dendra_api_client.requests.get')
    def test_get_meta_station_by_id(self, mock_get):
        station_id = 'aaa111bbb222ccc333ddd444'
        mock_get.return_value = _mock_response([{'_id': station_id, 'name': 'Test Station', 'slug': 'test-station'}])
        result = dendra.get_meta_station_by_id(station_id)
        self.assertEqual(result['name'], 'Test Station')
        self.assertEqual(mock_get.call_args[1]['params']['_id'], station_id)

    @patch('dendra_api_client.requests.get')
    def test_get_meta_datastream_by_id(self, mock_get):
        ds_id = 'aaa111bbb222ccc333ddd444'
        mock_get.return_value = _mock_response([{'_id': ds_id, 'name': 'Air Temp Avg', 'station_id': 'bbb222ccc333ddd444eee555'}])
        result = dendra.get_meta_datastream_by_id(ds_id)
        self.assertEqual(result['name'], 'Air Temp Avg')

    @patch('dendra_api_client.requests.get')
    def test_get_meta_annotation(self, mock_get):
        ann_id = 'aaa111bbb222ccc333ddd444'
        mock_get.return_value = _mock_response([{'_id': ann_id, 'title': 'Test Annotation'}])
        result = dendra.get_meta_annotation(ann_id)
        self.assertEqual(result['title'], 'Test Annotation')

    def test_get_meta_organization_no_args_raises(self):
        with self.assertRaises(ValueError):
            dendra.get_meta_organization()

    @patch('dendra_api_client.requests.get')
    def test_get_meta_organization_by_slug(self, mock_get):
        org_data = [{'_id': 'abc123def456abc123def456', 'name': 'ERCZO'}]
        mock_get.side_effect = [
            _mock_response([{'_id': 'abc123def456abc123def456'}]),  # get_organization_id
            _mock_response(org_data),                               # get_meta_organization
        ]
        result = dendra.get_meta_organization('erczo')
        self.assertEqual(result['name'], 'ERCZO')

    def test_get_datastream_by_id_delegates(self):
        ds_id = 'aaa111bbb222ccc333ddd444'
        with patch('dendra_api_client.get_meta_datastream_by_id') as mock_meta:
            mock_meta.return_value = {'_id': ds_id}
            result = dendra.get_datastream_by_id(ds_id)
            mock_meta.assert_called_once_with(ds_id, None)
            self.assertEqual(result['_id'], ds_id)


class TestGetDatapoints(unittest.TestCase):

    DS_ID = 'aaa111bbb222ccc333ddd444'
    STN_ID = 'bbb222ccc333ddd444eee555'

    def setUp(self):
        self._saved = dendra.headers.copy()

    def tearDown(self):
        dendra.headers.clear()
        dendra.headers.update(self._saved)

    def test_invalid_datastream_id_type_raises(self):
        with self.assertRaises(TypeError):
            dendra.get_datapoints(12345, '2019-01-01T00:00:00')

    def test_invalid_datastream_id_length_raises(self):
        with self.assertRaises(ValueError):
            dendra.get_datapoints('short', '2019-01-01T00:00:00')

    @patch('dendra_api_client.requests.get')
    def test_get_datapoints_with_explicit_name(self, mock_get):
        dp_data = [
            {'lt': '2019-02-01T08:00:00', 't': '2019-02-01T16:00:00.000Z', 'v': 15.2},
            {'lt': '2019-02-01T08:05:00', 't': '2019-02-01T16:05:00.000Z', 'v': 15.4},
        ]
        mock_get.side_effect = [
            _mock_response(dp_data),  # page 1
            _mock_response([]),       # page 2 (empty, stops paging)
        ]
        result = dendra.get_datapoints(self.DS_ID, '2019-02-01T00:00:00', '2019-03-01T00:00:00', name='MyStream')
        self.assertEqual(len(result), 2)
        self.assertIn('MyStream', result.columns)

    @patch('dendra_api_client.requests.get')
    def test_get_datapoints_default_name_uses_meta(self, mock_get):
        dp_data = [
            {'lt': '2019-02-01T08:00:00', 't': '2019-02-01T16:00:00.000Z', 'v': 15.2},
        ]
        ds_meta = [{'_id': self.DS_ID, 'name': 'Air Temp Avg', 'station_id': self.STN_ID}]
        stn_meta = [{'_id': self.STN_ID, 'slug': 'stunt-ranch'}]
        mock_get.side_effect = [
            _mock_response(dp_data),   # datapoints page 1
            _mock_response([]),        # datapoints page 2 (empty)
            _mock_response(ds_meta),   # get_meta_datastream_by_id
            _mock_response(stn_meta),  # get_meta_station_by_id
        ]
        result = dendra.get_datapoints(self.DS_ID, '2019-02-01T00:00:00', '2019-03-01T00:00:00')
        self.assertIn('StuntRanch_Air_Temp_Avg', result.columns)

    @patch('dendra_api_client.requests.get')
    def test_get_datapoints_empty_result(self, mock_get):
        ds_meta = [{'_id': self.DS_ID, 'name': 'Air Temp Avg', 'station_id': self.STN_ID}]
        stn_meta = [{'_id': self.STN_ID, 'slug': 'test-station'}]
        mock_get.side_effect = [
            _mock_response([]),        # empty datapoints
            _mock_response(ds_meta),   # get_meta_datastream_by_id
            _mock_response(stn_meta),  # get_meta_station_by_id
        ]
        result = dendra.get_datapoints(self.DS_ID, '2019-02-01T00:00:00', '2019-03-01T00:00:00')
        self.assertTrue(result.empty)

    @patch('dendra_api_client.requests.get')
    def test_get_datapoints_returns_status_code_on_http_error(self, mock_get):
        mock_get.return_value = _mock_response([], status_code=401)
        result = dendra.get_datapoints(self.DS_ID, '2019-02-01T00:00:00', '2019-03-01T00:00:00', name='Test')
        self.assertEqual(result, 401)

    @patch('dendra_api_client.requests.get')
    def test_get_datapoints_pages_through_results(self, mock_get):
        page1 = [{'lt': '2019-02-01T08:00:00', 't': '2019-02-01T16:00:00.000Z', 'v': i} for i in range(5)]
        page2 = [{'lt': '2019-02-01T09:00:00', 't': '2019-02-01T17:00:00.000Z', 'v': i} for i in range(3)]
        mock_get.side_effect = [
            _mock_response(page1),
            _mock_response(page2),
            _mock_response([]),  # empty final page
        ]
        result = dendra.get_datapoints(self.DS_ID, '2019-02-01T00:00:00', '2019-03-01T00:00:00', name='Test')
        self.assertEqual(len(result), 8)

    @patch('dendra_api_client.requests.get')
    def test_get_datapoints_needs_auth_blocks(self, mock_get):
        dendra.headers.pop('Authorization', None)
        with self.assertRaises(RuntimeError):
            dendra.get_datapoints(self.DS_ID, '2019-02-01T00:00:00', needs_auth=True)
        mock_get.assert_not_called()

    @patch('dendra_api_client.requests.get')
    def test_get_datapoints_from_station_id(self, mock_get):
        ds_list = [{'_id': self.DS_ID, 'name': 'Air Temp Avg'}]
        dp_data = [{'lt': '2019-02-01T08:00:00', 't': '2019-02-01T16:00:00.000Z', 'v': 15.2}]
        stn_meta = [{'_id': self.STN_ID, 'slug': 'test-station'}]
        ds_meta = [{'_id': self.DS_ID, 'name': 'Air Temp Avg', 'station_id': self.STN_ID}]
        mock_get.side_effect = [
            _mock_response(ds_list),   # list_datastreams_by_station_id
            _mock_response(dp_data),   # get_datapoints page 1
            _mock_response([]),        # get_datapoints page 2 (empty)
            _mock_response(ds_meta),   # get_meta_datastream_by_id
            _mock_response(stn_meta),  # get_meta_station_by_id
        ]
        result = dendra.get_datapoints_from_station_id(self.STN_ID, '2019-02-01T00:00:00', '2019-03-01T00:00:00')
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)


if __name__ == '__main__':
    unittest.main()
