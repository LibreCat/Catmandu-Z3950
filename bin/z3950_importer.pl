#!/usr/bin/env perl
$|++;

use Catmandu::Importer::Z3950;
use Data::Dumper;
use MARC::Record;
use Getopt::Long;
use Time::HiRes;

my $host = 'lx2.loc.gov';
my $port = 210;
my $databaseName = 'lcdb';
my $preferredRecordSyntax = 'USMARC';
my $queryType = 'PQF';
my $num;
my $help;
my $query;
my $sleep;

GetOptions( "h"        => \$help ,
            "host=s"   => \$host , 
            "port=i"   => \$port ,
            "base=s"   => \$databaseName ,
            "syntax=s" => \$preferredRecordSyntax ,
            "type=s"   => \$queryType ,
            "sleep=i"  => \$sleep ,
            "num=i"    => \$num);

&usage if $help;

binmode(STDOUT,':encoding(UTF-8)');

if (@ARGV) {
    my $query = shift;
    my $importer = Catmandu::Importer::Z3950->new(
                    host => $host ,
                    port => $port ,
                    databaseName => $databaseName ,
                    preferredRecordSyntax => $preferredRecordSyntax ,
                    queryType => $queryType ,
                    query => $query);

    my $n = $importer->slice(0,$num)->each(sub {
	    process_record(0,$_[0]);
    });

    print STDERR "processed: $n record\n";
}
else {
   while (<STDIN>) {
       chomp;
       my ($prefix,$query) = split(/\s+/,$_,2);
       my $importer = Catmandu::Importer::Z3950->new(
                    host => $host ,
                    port => $port ,
                    databaseName => $databaseName ,
                    preferredRecordSyntax => $preferredRecordSyntax ,
                    queryType => $queryType ,
                    query => $query);

       $importer->slice(0,$num)->each(sub {
            process_record($prefix,$_[0]);
       });

       Time::HiRes::usleep($sleep) if $sleep;
   }
}

sub process_record {
   my ($prefix,$ref) = @_;
   my $marc = MARC::Record->new_from_usmarc($ref);

   printf "%-9.9d %-3.3s   L BK\n" , $prefix, 'FMT';

   for my $field ($marc->fields) {
      my $tag  = $field->tag;
      my $ind1 = $field->indicator(1) || ' ';
      my $ind2 = $field->indicator(2) || ' ';

      printf "%-9.9d %-3.3s%s%s L " , $prefix, $tag, $ind1, $ind2; 
     
      if ($field->is_control_field) {
          printf "%s" , $field->data;
      }
      else {
          for my $subfield ($field->subfields) {
             printf "\$\$%s%s" , $subfield->[0], $subfield->[1];
          } 
      }

      print "\n";
   }
}

sub usage {
   print STDERR <<EOF;
usage: 

 $0 [options] query
 $0 [options] < file

where: file like
   prefix query
   prefix query
   prefix query
   .
   .
   .

options:
   -h 
   --host=$host
   --port=$port
   --base=$databaseName
   --syntax=$preferredRecordSyntax
   --type=$queryType
   --num=<max_number_of_records>
   --sleep=<micro_seconds>
   --prefix=$prefix

example:

   --host lx2.loc.gov --port 210 --base lcdb --type PQF

   query: \@attr 1=7 9781615300068  (search for 9781615300068 in ISBN-keyword)
   query: \@attr 1=8 0028-0836 (search for 0028-0836 in ISSN)

   Use: http://www.loc.gov/z3950/lcdbz3950.xml for more options 

usage:

   $0 "\@attr 1=7 9781615300068"
   $0 < example.txt
EOF
   exit 1;
}
