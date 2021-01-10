({
    appDir: '../WebContent',
    optimize: 'none',
    wrapShim: false,
    waitSeconds: 0,
    baseUrl: 'js/libs',
    paths: {
    	app: '../app',
     	i18n: '../../../../smapServer/WebContent/js/libs/i18n',
     	async: '../../../../smapServer/WebContent/js/libs/async',
     	localise: '../../../../smapServer/WebContent/js/app/localise',
    	jquery: '../../../../smapServer/WebContent/js/libs/jquery-2.1.1',
	bootstrap: '../../../../smapServer/WebContent/js/libs/bootstrap.min',
    	modernizr: '../../../../smapServer/WebContent/js/libs/modernizr',
    	rmm: '../../../../smapServer/WebContent/js/libs/responsivemobilemenu',
    	common: '../../../../smapServer/WebContent/js/app/common',
    	data: '../../../../smapServer/WebContent/js/app/data',
        moment: '../../../../smapServer/WebContent/js/libs/moment-with-locales.min',
    	version: '../../../../smapServer/WebContent/js/app/version',
    	pacesettings: '../../../../smapServer/WebContent/js/libs/paceSettings',
	pace: '../../../../smapServer/WebContent/js/libs/wb/plugins/pace/pace.min',
    	globals: '../../../../smapServer/WebContent/js/app/globals',
    	tablesorter: '../../../../smapServer/WebContent/js/libs/tablesorter',
    	crf: '../../../../smapServer/WebContent/js/libs/commonReportFunctions',
    	lang_location: '../../../../smapServer/WebContent/js'
    },
    dir: '../myWork',
    modules: [
        {
            name: '../my_work',
	    exclude: ['jquery', 'bootstrap']
        }
    ]
})
