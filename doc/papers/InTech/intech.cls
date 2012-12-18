%%% -----------------------------------------------------------------------------
% LaTeX class for the IN-TECH Web book class 
% 
% Dr. Alexander Förster, IDSIA, Manno, Switzerland @ 2009
%
%	**  Technical support: <debeljuh@intechweb.org>	**
%
% -----------------------------------------------------------------------------
%%
%% This file is 'intech.cls'
%% 
%% It may be distributed and/or modified under the
%% conditions of the LaTeX Project Public License, either version 1.3c
%% of this license or (at your option) any later version.
%% The latest version of this license is in
%%    http://www.latex-project.org/lppl.txt
%% and version 1.3c or later is part of all distributions of LaTeX
%% version 2005/12/01 or later.

%%% identification
\NeedsTeXFormat{LaTeX2e}[1995/12/01]
\ProvidesClass{intech}[2009/12/15 v. 1.10 IN-TECH Web book class]

\ProcessOptions \relax
\typeout{**************************************************************************}
\typeout{*** For report bugs, comments, suggestions, and improvements visit:}
\typeout{***        http://www.intechweb.org/}
\typeout{*** For any kind of help contact in-tech web or your corresponding}
\typeout{*** book editor.}
\typeout{*** For help contact Technical support: <debeljuh@intechweb.org> ***}
\typeout{**************************************************************************}
%%% package loading
\LoadClass[twoside]{article}

%%% main code
\RequirePackage{ifthen}
\RequirePackage[papersize={17cm,24cm},top={2,5cm},bottom={3cm},hmargin={2cm},nohead,nofoot,dvips,pdftex]{geometry}
\RequirePackage{amsfonts,amstext,amssymb,amsmath,amsthm,amscd,bm,paralist,color}
\RequirePackage{graphicx,array}
\RequirePackage{epstopdf}
\RequirePackage{mathpazo}
\RequirePackage[scaled=.95]{helvet}
\RequirePackage{courier}
\RequirePackage[T1]{fontenc}
\RequirePackage{textcomp}
\RequirePackage[sort&compress]{natbib}
\RequirePackage[english]{babel}
\RequirePackage[singlelinecheck=false,figurename=Fig.,labelsep=period,skip=2pt]{caption}
\RequirePackage{subfigure}
\RequirePackage[bottom]{footmisc}
\RequirePackage{float,verbatim}
\RequirePackage{fancyhdr}
\RequirePackage{calc}

\RequirePackage[breaklinks,hyperfootnotes=false]{hyperref}

\newboolean{@booktitle}
\newboolean{@secondauthors}
\newboolean{@secondaffiliation}
\newboolean{@secondcountry}
\newboolean{@thirdauthors}
\newboolean{@thirdaffiliation}
\newboolean{@thirdcountry}

\fancypagestyle{plain}{%
  \fancyhf{}%
  \renewcommand{\headrulewidth}{0pt}%
  \renewcommand{\footrulewidth}{0pt}%
}

\pagestyle{empty}

\raggedbottom
\addtolength{\topskip}{0pt plus 12pt}
\setlength{\parindent}{0ex}

\widowpenalty=9999
\clubpenalty=9999

\newlength{\myFootnoteWidth}
\newlength{\myFootnoteLabel}
\setlength{\myFootnoteLabel}{0.8em}%  <-- can be changed to any valid value
\renewcommand{\@makefntext}[1]{%
  \setlength{\myFootnoteWidth}{\columnwidth}%
  \addtolength{\myFootnoteWidth}{-\myFootnoteLabel}%
  \noindent\makebox[\myFootnoteLabel][r]{\@makefnmark\ }%
  \parbox[t]{\myFootnoteWidth}{#1}%
}

\newcommand{\booktitle}[1]{\gdef\@booktitle{#1}\setboolean{@booktitle}{true}}

\newcommand{\chaptertitle}[1]{\gdef\@chaptertitle{#1}}
\newcommand{\@chaptertitle}{\@latex@error{No \noexpand\chaptertitle given}\@ehc}

\newcommand{\authors}[1]{\gdef\@authors{#1}}
\newcommand{\@authors}{\@latex@error{No \noexpand\authors given}\@ehc}

\newcommand{\affiliation}[1]{\gdef\@affiliation{#1}}
\newcommand{\@affiliation}{\@latex@error{No \noexpand\affiliation given}\@ehc}

\newcommand{\country}[1]{\gdef\@country{#1}}
\newcommand{\@country}{\@latex@error{No \noexpand\country given}\@ehc}

\newcommand{\secondauthors}[1]{\gdef\@secondauthors{#1}\setboolean{@secondauthors}{true}}
\newcommand{\secondaffiliation}[1]{\gdef\@secondaffiliation{#1}\setboolean{@secondaffiliation}{true}}
\newcommand{\secondcountry}[1]{\gdef\@secondcountry{#1}\setboolean{@secondcountry}{true}}

\newcommand{\thirdauthors}[1]{\gdef\@thirdauthors{#1}\setboolean{@thirdauthors}{true}}
\newcommand{\thirdaffiliation}[1]{\gdef\@thirdaffiliation{#1}\setboolean{@thirdaffiliation}{true}}
\newcommand{\thirdcountry}[1]{\gdef\@thirdcountry{#1}\setboolean{@thirdcountry}{true}}

\newcounter{chapter}
\setcounter{chapter}{1}

\def\@sect#1#2#3#4#5#6[#7]#8{%
  \ifnum #2>\c@secnumdepth
    \let\@svsec\@empty
  \else
    \refstepcounter{#1}%
    \protected@edef\@svsec{\csname the#1\endcsname
\ifnum #2<1.\fi}%
  \fi
  \@tempskipa #5\relax
  \ifdim \@tempskipa>\z@
    \begingroup
      #6{%
        \@hangfrom{\hskip #3\relax\@svsec\hspace{1ex}}%
          \interlinepenalty \@M #8 \@@par}%
    \endgroup
    \csname #1mark\endcsname{#7}%
    \addcontentsline{toc}{#1}{%
      \ifnum #2>\c@secnumdepth \else
        \protect\numberline{\csname the#1\endcsname}%
      \fi
      #7}%
  \else
    \def\@svsechd{%
      #6{\hskip #3\relax
      \@svsec #8}%
      \csname #1mark\endcsname{#7}%
      \addcontentsline{toc}{#1}{%
        \ifnum #2>\c@secnumdepth \else
          \protect\numberline{\csname the#1\endcsname}%
        \fi
        #7}}%
  \fi
  \@xsect{#5}}

%% Define \normalsize as 9 pt which is normally \small 

\renewcommand\normalsize{%
   \@setfontsize\normalsize\@ixpt{11}%
   \abovedisplayskip 8.5\p@ \@plus3\p@ \@minus4\p@
   \abovedisplayshortskip \z@ \@plus2\p@
   \belowdisplayshortskip 4\p@ \@plus2\p@ \@minus2\p@
   \def\@listi{\leftmargin\leftmargini
               \topsep 4\p@ \@plus2\p@ \@minus2\p@
               \parsep 2\p@ \@plus\p@ \@minus\p@
               \itemsep \parsep}%
   \belowdisplayskip \abovedisplayskip
}
\renewcommand \small{%
   \@setfontsize \small\@viiipt{9.5}%
   \abovedisplayskip 6\p@ \@plus2\p@ \@minus4\p@
   \abovedisplayshortskip \z@ \@plus\p@
   \belowdisplayshortskip 3\p@ \@plus\p@ \@minus2\p@
   \def\@listi{\leftmargin\leftmargini
               \topsep 3\p@ \@plus\p@ \@minus\p@
               \parsep 2\p@ \@plus\p@ \@minus\p@
               \itemsep \parsep}%
   \belowdisplayskip \abovedisplayskip
}

\renewcommand\scriptsize{\@setfontsize\scriptsize\@viiipt\@ixpt}
\renewcommand\tiny{\@setfontsize\tiny\@vpt\@vipt}
\renewcommand\large{\@setfontsize\large\@xpt\@xiipt}
\renewcommand\Large{\@setfontsize\Large\@xipt{13.6}}
\renewcommand\LARGE{\@setfontsize\LARGE{16}{20.6}}
\renewcommand\huge{\@setfontsize\huge{18}{23}}
\renewcommand\Huge{\@setfontsize\Huge\@xxvpt{30}}

\setcounter{secnumdepth}{5}%
\setcounter{tocdepth}{5}%

\renewcommand{\@dotsep}{0}
\renewcommand*\l@section{\@dottedtocline{1}{0em}{1.5em}}
\renewcommand*\l@subsection{\@dottedtocline{2}{1.5em}{2.3em}}
\renewcommand*\l@subsubsection{\@dottedtocline{3}{3.8em}{3.2em}}
\renewcommand*\l@paragraph{\@dottedtocline{4}{7.0em}{4.1em}}
\renewcommand*\l@subparagraph{\@dottedtocline{5}{10em}{5em}}

\renewcommand{\section}{\@startsection%
  {section}%
  {0}%
  {0em}%
  {-\baselineskip}%
  {0.5\baselineskip}%
  {\fontfamily{phv}\bfseries\large }}%

\renewcommand{\subsection}{\@startsection%
    {subsection}%
  {1}%
  {0em}%
  {-\baselineskip}%
  {0.01\baselineskip}%
  {\fontfamily{phv}\bfseries\normalsize}}%

\renewcommand{\subsubsection}{\@startsection%
  {subsubsection}%
  {2}%
  {0em}%
  {-\baselineskip}%
  {0.01\baselineskip}%
  {\fontfamily{phv}\bfseries\normalsize}}%

\renewcommand{\paragraph}{\@startsection%
  {paragraph}%
  {3}%
  {0em}%
  {-\baselineskip}%
  {0.01\baselineskip}%
  {\fontfamily{phv}\bfseries\normalsize}}%

\renewcommand{\subparagraph}{\@startsection%
  {subparagraph}%
  {4}%
  {0.7cm}%
  {-\baselineskip}%
  {0.5\baselineskip}%
  {\normalfont\normalsize\itshape\sffamily}}%
  
\def\@maketitle{\newpage
  \begin{flushright}%
  {\bfseries\huge\fontfamily{phv}\selectfont \arabic{chapter}}\\*[22pt]%
  {\bfseries\LARGE\fontfamily{phv}\selectfont\@chaptertitle} \\  \vspace*{12pt}%
 {\Large \@authors\par}%   
 {\large  \emph{\@affiliation}\par}%  
 {\large \emph{\@country}\par}%
  \ifthenelse{\boolean{@secondauthors}}{\vskip\baselineskip%
 {\Large \@secondauthors\par}%   
 {\large  \emph{\@secondaffiliation}\par}%  
 {\large \emph{\@secondcountry}\par}%
    }{}%
  \ifthenelse{\boolean{@thirdauthors}}{\vskip\baselineskip%
 {\Large \@thirdauthors\par}  
 {\large  \emph{\@thirdaffiliation}\par}  
 {\large \emph{\@thirdcountry}\par}%
    }{}%
    \vspace{20pt}
 \end{flushright}%
}%

%%% Set special bibliography and citation style
\renewcommand{\bibsection}{\section{References}}
\setlength{\bibhang}{1.24cm}
\setlength{\bibsep}{0cm}
\bibliographystyle{dcu}
\setcitestyle{authoryear,round,citesep={;},aysep={,},yysep={;}}
\gdef\harvardand{\&}
