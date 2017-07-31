#include <cstdio>
#include <map>
#include <set>
#include <vector>
#include <utility>
#include <cassert>
#include <string>
#include <algorithm>
#include <cmath>

using namespace std;


typedef long long int ll;


const ll printStart = 1501520574ll;
const ll printEnd   = 1501520916ll;



int main(int argc, char *argv[]) {

  int numFiles = argc - 1;

  vector<map<ll,double>> all;
  vector<ll> starts(numFiles, 1000000000000ll), ends(numFiles, -1);

  for(int i=0; i<numFiles; i++) {
    //fprintf(stderr,"a0\n");
    FILE *f = fopen(argv[i+1], "r");

    // comment lines at the beginning
    for(int j=0; j<6; j++){
      char buf[1024];
      fgets(buf,1024,f);
      assert(buf[0]=='"');
    }

    //fprintf(stderr,"b1\n");

    while(1){
      double epoch, usr, sys, idl, wai, stl;
      int r = fscanf(f,"%lf,%lf,%lf,%lf,%lf,%lf\n",&epoch,&usr,&sys,&idl,&wai,&stl);
      if (r == EOF) break;

      //fprintf(stderr,"b2\n");

      all.emplace_back();
      ll iepoch = round(epoch);
      all[i][iepoch]=usr+sys;
      assert(usr+sys >= 0 && usr+sys <= 100);
      
      //fprintf(stderr,"b3\n");

      starts[i] = min(starts[i], iepoch);
      ends[i] = max(ends[i], iepoch);

      //fprintf(stderr,"a1\n");
    }
  }

  //fprintf(stderr,"a2\n");

  ll start = -1, end = 1000000000000ll;
  for(int i=0; i<numFiles; i++){
    fprintf(stderr,"starts[i]: %lld, ends[i]: %lld\n", starts[i], ends[i]);
    start = max(start, starts[i]);
    end = min(end, ends[i]);
  }

  fprintf(stderr,"start: %lld, end: %lld\n", start, end);

  //fprintf(stderr,"a3\n");

  map<ll,double> summed;
  for(ll t = start; t<=end; t++) {
    double &s = summed[t];
    for(int i=0; i<numFiles; i++) {
      s += all[i][t];
    }
    s /= numFiles;
  }

  assert(printStart>start && printEnd<end);

  //fprintf(stderr,"a4\n");
  
  printf("epoch\tutil\n");
  int i=0;
  //for(pair<ll,double> m: summed) {
  for(ll t=printStart; t<=printEnd; t++) {
    //printf("%lld\t%lf\n", m.first, m.second);
    printf("%d\t%lf\n", i++, summed[t]);
  }

  return 0;
}
