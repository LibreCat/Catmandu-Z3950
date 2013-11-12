use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name => 'Catmandu::Z3950',
    license => 'perl',
    dist_author => [
        'Wouter Willaert <wouterw@inuits.eu>'
    ],
    dist_version_from => 'lib/Catmandu/Z3950.pm',
    build_requires => {
        'Software::License' => 0,
        'Test::Exception' => 0,
        'Test::More' => 0,
    },
    requires => {
        'perl' => '5.10.1',
        'Catmandu' => '0.0302',
        'Moo' => '1.000003',
		'ZOOM' => 0
    },
    add_to_cleanup => [qw(
        Catmandu-Z3950*
    )],
    create_makefile_pl => 'traditional',
    create_license => 1,
);

$builder->create_build_script;
