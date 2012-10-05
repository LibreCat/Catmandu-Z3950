package Catmandu::Importer::Z3950;

use Catmandu::Sane;
use Moo;
use ZOOM;

with 'Catmandu::Importer';


# INFO:
# http://www.loc.gov/z3950/


# Constants. -------------------------------------------------------------------

use constant PREFERREDRECORDSYNTAX => 'USMARC';
use constant PORT => 210;
use constant QUERYTYPE => 'CQL';


# Properties. ------------------------------------------------------------------

# required.
has host => (is => 'ro', required => 1);
has databaseName => (is => 'ro', required => 1);
has query => (is => 'ro', required => 1);

# optional.
has port => (is => 'ro', default => sub { return PORT; });
has preferredRecordSyntax => (is => 'ro', default => sub { return PREFERREDRECORDSYNTAX; });
has user => (is => 'ro');
has password => (is => 'ro');
has queryType => (is =>'ro', default => sub { return QUERYTYPE; }); # <CQL | PQF>

# internal stuff.
has _currentRecordSet => (is => 'ro');
has _n => (is => 'ro');

# Internal Methods. ------------------------------------------------------------

sub _setup_connection {
  my ($self) = @_;

  my $conn = ZOOM::Connection->new(
    $self->host,
    $self->port,
    databaseName => $self->databaseName
  );

  $conn->option(preferredRecordSyntax => $self->preferredRecordSyntax) if $self->preferredRecordSyntax;
  $conn->option(user => $self->user) if $self->user;
  $conn->option(password => $self->password) if $self->password;

  return $conn;
}

sub _get_query {
  my ($self) = @_;
  my $qry;

  if ($self->queryType eq 'CQL') {
    $qry = ZOOM::Query::CQL->new($self->query); # 'title=dinosaur'
  }
  elsif ($self->queryType eq 'PQF') {
    $qry = ZOOM::Query::PQF->new($self->query); # '@attr 1=4 dinosaur'
  }

  return $qry;
}

sub _nextRecord {
  my ($self) = @_;
  
  unless ($self->_currentRecordSet) {
    $self->{_currentRecordSet} = $self->_setup_connection->search($self->_get_query);
  }

  return $self->_currentRecordSet->record($self->{_n}++)->get("raw");
}


# Public Methods. --------------------------------------------------------------

sub generator {
  my ($self) = @_;

  return sub {
    $self->_nextRecord;
  };
}


# PerlDoc. ---------------------------------------------------------------------

=head1 NAME

  Catmandu::Importer::Z3950 - Package that imports Z3950 data

  Returns the raw response. So the output depends on the preferredRecordSyntax.
  You could use the MARC::Record package to parse the MARC output blob.

=head1 SYNOPSIS

  use Catmandu::Importer::Z3950;

  my %attrs = (
    host => 'z3950.loc.gov',
    port => 7090,
    databaseName => "Voyager",
    preferredRecordSyntax => "USMARC",
    queryType => 'PQF', # CQL or PQF
    query => '@attr 1=4 dinosaur'
  );

  my $importer = Catmandu::Importer::Z3950->new(%attrs);

  my $n = $importer->each(sub {
    my $hashref = $_[0];
    ...
  });

  print "DONE. ($n). \n";

  `host`, `database` and `query` are required, `user` and `user` are optional.
  `port`, `preferredRecordSyntax` and `queryType` will default to 210 'USMARC' and 'CQL' respectively.

=cut

=head1 REMARK

This package uses the ZOOM package internally.
For more info visit: http://search.cpan.org/~mirk/Net-Z3950-ZOOM-1.28/lib/ZOOM.pod

The ZOOM package has a hard dependency on YAZ toolkit.
For more info about YAZ, visit: https://www.indexdata.com/yaz

Installing YAZ:
- (osx, using homebrew): brew install yaz
- (linux, using yum): yum install yaz libyaz

=head1 SEE ALSO

L<Catmandu::Iterable>

=cut

1;
