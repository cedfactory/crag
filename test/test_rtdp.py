import pytest
from src import rtdp_tv

class TestRTDP:
    def test_rtdp_tv_next_complete(self, mocker):
        
        # context
        rtdptv = rtdp_tv.RTDPTradingView()

        # action
        selection = rtdptv.next()

        # expectation
        assert(isinstance(selection, list))
    
    def test_rtdp_tv_next(self, mocker):
        
        # context
        mock_rtdp_tv = rtdp_tv.RTDPTradingView()
        response = '{"elapsed_time":"0:00:03.581386","result":{"status":"ok","symbols":"{\\"symbol\\":{\\"0\\":\\"GAL\\\\/USD\\",\\"1\\":\\"AAVE\\\\/USD\\",\\"2\\":\\"SKL\\\\/USD\\",\\"3\\":\\"CHZ\\\\/USD\\",\\"4\\":\\"CONV\\\\/USD\\",\\"5\\":\\"1INCH\\\\/USD\\",\\"6\\":\\"BILI\\\\/USD\\",\\"7\\":\\"RUNE\\\\/USD\\",\\"8\\":\\"SNX\\\\/USD\\",\\"9\\":\\"CRV\\\\/USD\\",\\"10\\":\\"JOE\\\\/USD\\",\\"11\\":\\"CITY\\\\/USD\\",\\"12\\":\\"SOL\\\\/USD\\",\\"13\\":\\"AVAX\\\\/USD\\",\\"14\\":\\"BAR\\\\/USD\\",\\"15\\":\\"STSOL\\\\/USD\\",\\"16\\":\\"PROM\\\\/USD\\",\\"17\\":\\"SQ\\\\/USD\\",\\"18\\":\\"MAPS\\\\/USD\\",\\"19\\":\\"MSOL\\\\/USD\\"},\\"change1h\\":{\\"0\\":23.6842105263,\\"1\\":10.9326169198,\\"2\\":6.5445026178,\\"3\\":6.5140764798,\\"4\\":3.825136612,\\"5\\":2.9719800406,\\"6\\":2.7331738982,\\"7\\":2.2191803551,\\"8\\":2.1256123163,\\"9\\":1.8638925011,\\"10\\":1.82727866,\\"11\\":1.7421035493,\\"12\\":1.5266501941,\\"13\\":1.469840732,\\"14\\":1.411233418,\\"15\\":1.3972484953,\\"16\\":1.2569130216,\\"17\\":1.2127697521,\\"18\\":1.1219512195,\\"19\\":1.0398981324},\\"rank_change1h\\":{\\"0\\":0,\\"1\\":1,\\"2\\":2,\\"3\\":3,\\"4\\":5,\\"5\\":7,\\"6\\":8,\\"7\\":9,\\"8\\":10,\\"9\\":13,\\"10\\":15,\\"11\\":18,\\"12\\":23,\\"13\\":25,\\"14\\":26,\\"15\\":27,\\"16\\":33,\\"17\\":34,\\"18\\":39,\\"19\\":44},\\"change24h\\":{\\"0\\":40.0059577003,\\"1\\":18.5851600047,\\"2\\":17.0212765957,\\"3\\":11.4162655553,\\"4\\":11.9842829077,\\"5\\":4.4670672007,\\"6\\":5.4347826087,\\"7\\":16.8911867944,\\"8\\":4.7179119204,\\"9\\":5.7863640626,\\"10\\":11.3463368221,\\"11\\":6.9301848049,\\"12\\":4.5338906051,\\"13\\":5.2729879533,\\"14\\":4.4173205464,\\"15\\":4.2661361627,\\"16\\":4.0289256198,\\"17\\":8.0540746382,\\"18\\":8.8855817211,\\"19\\":4.270696452},\\"rank_change24h\\":{\\"0\\":1,\\"1\\":5,\\"2\\":7,\\"3\\":11,\\"4\\":10,\\"5\\":41,\\"6\\":30,\\"7\\":8,\\"8\\":35,\\"9\\":26,\\"10\\":13,\\"11\\":22,\\"12\\":38,\\"13\\":31,\\"14\\":43,\\"15\\":46,\\"16\\":48,\\"17\\":21,\\"18\\":19,\\"19\\":45},\\"RECOMMENDATION_15m\\":{\\"0\\":\\"STRONG_BUY\\",\\"1\\":\\"STRONG_BUY\\",\\"2\\":\\"BUY\\",\\"3\\":\\"BUY\\",\\"4\\":\\"BUY\\",\\"5\\":\\"BUY\\",\\"6\\":\\"BUY\\",\\"7\\":\\"BUY\\",\\"8\\":\\"STRONG_BUY\\",\\"9\\":\\"BUY\\",\\"10\\":\\"STRONG_BUY\\",\\"11\\":\\"BUY\\",\\"12\\":\\"STRONG_BUY\\",\\"13\\":\\"STRONG_BUY\\",\\"14\\":\\"BUY\\",\\"15\\":\\"STRONG_BUY\\",\\"16\\":\\"BUY\\",\\"17\\":\\"BUY\\",\\"18\\":\\"BUY\\",\\"19\\":\\"STRONG_BUY\\"},\\"RECOMMENDATION_30m\\":{\\"0\\":\\"STRONG_BUY\\",\\"1\\":\\"STRONG_BUY\\",\\"2\\":\\"STRONG_BUY\\",\\"3\\":\\"BUY\\",\\"4\\":\\"STRONG_BUY\\",\\"5\\":\\"STRONG_BUY\\",\\"6\\":\\"STRONG_BUY\\",\\"7\\":\\"BUY\\",\\"8\\":\\"STRONG_BUY\\",\\"9\\":\\"STRONG_BUY\\",\\"10\\":\\"BUY\\",\\"11\\":\\"STRONG_BUY\\",\\"12\\":\\"STRONG_BUY\\",\\"13\\":\\"STRONG_BUY\\",\\"14\\":\\"BUY\\",\\"15\\":\\"STRONG_BUY\\",\\"16\\":\\"BUY\\",\\"17\\":\\"STRONG_BUY\\",\\"18\\":\\"STRONG_BUY\\",\\"19\\":\\"STRONG_BUY\\"},\\"RECOMMENDATION_1h\\":{\\"0\\":\\"STRONG_BUY\\",\\"1\\":\\"STRONG_BUY\\",\\"2\\":\\"STRONG_BUY\\",\\"3\\":\\"STRONG_BUY\\",\\"4\\":\\"STRONG_BUY\\",\\"5\\":\\"STRONG_BUY\\",\\"6\\":\\"STRONG_BUY\\",\\"7\\":\\"STRONG_BUY\\",\\"8\\":\\"STRONG_BUY\\",\\"9\\":\\"STRONG_BUY\\",\\"10\\":\\"BUY\\",\\"11\\":\\"STRONG_BUY\\",\\"12\\":\\"STRONG_BUY\\",\\"13\\":\\"STRONG_BUY\\",\\"14\\":\\"STRONG_BUY\\",\\"15\\":\\"STRONG_BUY\\",\\"16\\":\\"STRONG_BUY\\",\\"17\\":\\"STRONG_BUY\\",\\"18\\":\\"STRONG_BUY\\",\\"19\\":\\"STRONG_BUY\\"}}"},"status":"ok"}'
        mocker.patch.object(mock_rtdp_tv, "_fetch_data", return_value=response)

        # action
        selection = mock_rtdp_tv.next()

        # expectation
        expected_selection = ['GAL/USD', 'AAVE/USD', 'SKL/USD', 'CHZ/USD', 'CONV/USD', '1INCH/USD', 'BILI/USD', 'RUNE/USD', 'SNX/USD', 'CRV/USD', 'JOE/USD', 'CITY/USD', 'SOL/USD', 'AVAX/USD', 'BAR/USD', 'STSOL/USD', 'PROM/USD', 'SQ/USD', 'MAPS/USD', 'MSOL/USD']
        assert(selection == expected_selection)
    
    def test_rtdp_tv_next_ko(self, mocker):
        
        # context
        mock_rtdp_tv = rtdp_tv.RTDPTradingView()
        response = '{"elapsed_time":"0:00:03.581386","result":{},"status":"ko","reason":"problem when fetching data"}'
        mocker.patch.object(mock_rtdp_tv, "_fetch_data", return_value=response)

        # action
        selection = mock_rtdp_tv.next()

        # expectation
        assert(len(selection) == 0)
    
