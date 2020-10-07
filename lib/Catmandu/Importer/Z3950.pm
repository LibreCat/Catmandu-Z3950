package Catmandu::Importer::Z3950;

use Catmandu::Sane;
use Catmandu::Util qw(:is);
use Scalar::Util qw(blessed);
use Carp;
use Moo;
use ZOOM;

our $VERSION = '0.06';

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
has query => (is => 'rw');

# optional.
has port => (is => 'ro', default => sub { return PORT; });
has preferredRecordSyntax => (is => 'ro', default => sub { return PREFERREDRECORDSYNTAX; });
has user => (is => 'ro');
has password => (is => 'ro');
has queryType => (is =>'ro', default => sub { return QUERYTYPE; }); # <CQL | PQF>
has handler => (is => 'rw', lazy => 1 , builder => 1, coerce => \&_coerce_handler );

# internal stuff.
has _conn => (is => 'ro');
has _qry => (is => 'ro');
has _currentRecordSet => (is => 'ro');
has _n => (is => 'ro', default => sub { 0 });

# Internal Methods. ------------------------------------------------------------

sub _build_handler {
    my ($self) = @_;

    if ($self->preferredRecordSyntax eq 'USMARC') {
        Catmandu::Util::require_package('Catmandu::Importer::Z3950::Parser::USMARC')->new;
    }
    else {
      return sub { return { record => $_[0] } };
    }
}

sub _coerce_handler {
  my ($handler) = @_;

  return $handler if is_invocant($handler) or is_code_ref($handler);

  if ($handler eq 'RAW') {
      return sub { return { record => $_[0] } };
  }
  elsif (is_string($handler) && !is_number($handler)) {
      my $class = $handler =~ /^\+(.+)/ ? $1
        : "Catmandu::Importer::Z3950::Parser::$handler";

      my $handler;
      eval {
          $handler = Catmandu::Util::require_package($class)->new;
      };
      if ($@) {
        croak $@;
      } else {
        return $handler;
      }
  }
  else {
      die "unknown handler type $handler";
  }
}

sub _setup_connection {
  my ($self) = @_;

	my $opts = ZOOM::Options->new();
	$opts->option(databaseName => $self->databaseName);
  $opts->option(preferredRecordSyntax => $self->preferredRecordSyntax) if $self->preferredRecordSyntax;
  $opts->option(user => $self->user) if $self->user;
  $opts->option(password => $self->password) if $self->password;

	my $conn = ZOOM::Connection->create($opts);
  $conn->connect(
    $self->host,
    $self->port,
  );

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

  unless ($self->_conn) {
    $self->_clean;
    $self->{_conn} = $self->_setup_connection;
  }

  unless ($self->_qry) {
    $self->_clean;
    $self->{_qry} = $self->_get_query;
  }

  unless ($self->_currentRecordSet) {
    $self->{_currentRecordSet} = $self->{_conn}->search($self->{_qry});
    $self->{_n} = 0;
  }

  my $size = $self->_currentRecordSet->size() || 0;

  if ($self->{_n} < $size) {
    my $rec = $self->_currentRecordSet->record($self->{_n}++)->get("raw");
    return blessed($self->handler)
         ? $self->handler->parse($rec)
         : $self->handler->($rec);
  }
  else {
    $self->_clean;
    return undef;
  }
}

sub _clean {
   my ($self) = @_;
   $self->{_currentRecordSet}->destroy() if $self->{_currentRecordSet};
   $self->{_qry}->destroy() if $self->{_qry};
   $self->{_currentRecordSet} = undef;
   $self->{_qry} = undef;
   $self->{_n} = 0;
}

sub DESTROY {
  my ($self) = @_;

  if ($self->_conn) {
     $self->_conn->destroy();
  }
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

=head1 SYNOPSIS

  # On the command line

  $ catmandu convert Z3950 --host z3950.loc.gov --port 7090 --databaseName Voyager --query "(title = dinosaur)"

  # From Perl

  use Catmandu;

  my $importer = Catmandu->importer('Z3950'
          host => 'z3950.loc.gov',
          port => 7090,
          databaseName => "Voyager",
          preferredRecordSyntax => "USMARC",
          queryType => 'PQF', # CQL or PQF
          query => '@attr 1=4 dinosaur'
  );

  my $n = $importer->each(sub {
    my $hashref = $_[0];
    ...
  });

=cut

=head1 CONFIGURATION

=over

=item host

The Z3950 host name

=item port

The Z3950 port

=item user

A user name

=item password

A password

=item databaseName

The database to connect to

=item preferredRecordSyntax

The preferred response format (default: USMARC)

=item queryType

The queryType (CQL or PQF)

=item query

The query

=item handler

The Perl handler to parse the response content. Examples are 'USMARC' and 'UNIMARC'

   $ catmandu convert Z3950 \
            --user 'XXX' \
            --password 'XXX' \
            --host z3950.bnf.fr \
            --port 2211 \
            --databaseName TOUT-ANA1-UTF8 \
            --preferredRecordSyntax Unimarc \
            --queryType PQF \
            --query '@attr 1=7 9782744024191' \
            --handler UNIMARC

=back

=head1 REQUIREMENTS

This package uses the ZOOM package internally.
For more info visit: L<http://search.cpan.org/~mirk/Net-Z3950-ZOOM-1.28/lib/ZOOM.pod>

The ZOOM package has a hard dependency on YAZ toolkit.
For more info about YAZ, visit: L<https://www.indexdata.com/yaz>

Installing YAZ:

=over

=item * (OS X, using Homebrew): C<brew install yaz>

=item * (Linux, using yum): C<yum install yaz libyaz>

=back

=head1 AUTHOR

=over

=item * Wouter Willaert, C<< <wouterw@inuits.eu> >>

=item * Patrick Hochstenbach, C<< <patrick.hochstenbach@ugent.be> >>

=back

=head1 CONTRIBUTORS

=over

=item * Emmanuel Di Pretoro, C<< edipretoro at gmail.com >>

=back

=head1 SEE ALSO

L<Catmandu::Iterable>

=cut

1;
